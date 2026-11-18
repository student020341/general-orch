package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"orch/internal/model"
)

type apiClient struct {
	baseURL string
	http    *http.Client
}

func main() {
	defaultURL := os.Getenv("ORCH_GATEWAY_URL")
	if defaultURL == "" {
		defaultURL = "http://localhost:8090"
	}

	global := flag.NewFlagSet("orchctl", flag.ExitOnError)
	baseURL := global.String("url", defaultURL, "gateway base URL")
	global.Usage = printUsage
	_ = global.Parse(os.Args[1:])

	args := global.Args()
	if len(args) == 0 {
		printUsage()
		os.Exit(1)
	}

	c := &apiClient{
		baseURL: strings.TrimRight(*baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
	}

	var err error
	switch args[0] {
	case "status", "ls":
		err = c.cmdStatus()
	case "watch":
		err = c.cmdWatch(args[1:])
	case "plans":
		err = c.cmdPlans(args[1:])
	case "apply":
		err = c.cmdApply(args[1:])
	case "catalog":
		err = c.cmdCatalog(args[1:])
	default:
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `Usage: orchctl [--url=http://localhost:8090] <command>

Commands:
  status                         Show all entities (alias: ls)
  watch [--interval=1s]          Live monitor of entities and latest plan steps
  plans <entity_id>              Show plans and DAG steps for an entity
  apply <entity_id> [flags]      Submit an intent patch (--kind, --vendor, --queue, --patch)
  catalog ls                     List device profiles in the catalog
  catalog get <kind>             Get one device profile as JSON
  catalog put <kind> [flags]     Create/update a device profile (--file or --json)
  catalog delete <kind>          Delete a device profile
  catalog reload                 Reload catalog from configs/profiles.json and broadcast`)
}

func (c *apiClient) cmdStatus() error {
	entities, err := c.listEntities()
	if err != nil {
		return err
	}
	return renderEntities(os.Stdout, entities, nil)
}

func (c *apiClient) cmdWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	interval := fs.Duration("interval", time.Second, "poll interval")
	_ = fs.Parse(args)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	for {
		entities, err := c.listEntities()
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\033[H\033[2J")
		fmt.Fprintf(&buf, "orchctl watch (%s)  %s\n\n", c.baseURL, time.Now().Format("15:04:05"))
		if err != nil {
			fmt.Fprintf(&buf, "error fetching entities: %v\n", err)
		} else {
			latestPlans := make(map[string]*model.Plan, len(entities))
			for _, e := range entities {
				if plans, perr := c.listPlans(e.ID); perr == nil && len(plans) > 0 {
					latestPlans[e.ID] = plans[0]
				}
			}
			_ = renderEntities(&buf, entities, latestPlans)
		}
		_, _ = os.Stdout.Write(buf.Bytes())

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (c *apiClient) cmdPlans(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: orchctl plans <entity_id>")
	}
	plans, err := c.listPlans(args[0])
	if err != nil {
		return err
	}
	if len(plans) == 0 {
		fmt.Println("No plans found.")
		return nil
	}
	for _, p := range plans {
		fmt.Printf("Plan %s  v%d  status=%s  created=%s\n", p.ID, p.Version, p.Status, p.CreatedAt.Format(time.RFC3339))
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "  STEP\tSTATUS\tQUEUE\tCAPABILITY\tDEPENDS_ON\tERROR")
		for _, st := range p.Steps {
			deps := "-"
			if len(st.DependsOn) > 0 {
				deps = strings.Join(st.DependsOn, ",")
			}
			errCol := st.Error
			if errCol == "" {
				errCol = "-"
			}
			fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\t%s\t%s\n", st.ID, st.Status, st.TaskQueue, st.Capability, deps, errCol)
		}
		_ = tw.Flush()
		fmt.Println()
	}
	return nil
}

func (c *apiClient) cmdApply(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: orchctl apply <entity_id> [--kind=...] [--vendor=mock] [--queue=...] --patch='{...}'")
	}
	entityID := args[0]
	fs := flag.NewFlagSet("apply", flag.ExitOnError)
	kind := fs.String("kind", "", "entity kind")
	vendor := fs.String("vendor", "", "driver vendor")
	queue := fs.String("queue", "", "optional task queue override")
	patchStr := fs.String("patch", "", "JSON patch object")
	replaceFlag := fs.Bool("replace", false, "replace entire target state instead of merging patch")
	_ = fs.Parse(args[1:])

	if *patchStr == "" {
		return errors.New("--patch is required")
	}
	var patch model.State
	if err := json.Unmarshal([]byte(*patchStr), &patch); err != nil {
		return fmt.Errorf("invalid --patch json: %w", err)
	}

	body := map[string]any{
		"kind":       *kind,
		"vendor":     *vendor,
		"task_queue": *queue,
		"patch":      patch,
		"replace":    *replaceFlag,
	}
	var resp map[string]any
	if err := c.doJSON(http.MethodPut, "/entities/"+entityID+"/intent", body, &resp); err != nil {
		return err
	}
	return printJSON(resp)
}

func (c *apiClient) cmdCatalog(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: orchctl catalog <ls|get|put|delete|reload>")
	}
	switch args[0] {
	case "ls":
		var profiles []model.DeviceProfile
		if err := c.doJSON(http.MethodGet, "/catalog/profiles", nil, &profiles); err != nil {
			return err
		}
		tw := tabwriter.NewWriter(os.Stdout, 0, 2, 2, ' ', 0)
		fmt.Fprintln(tw, "KIND\tASPECTS\tLIVE_STATE\tSERIAL\tFETCH_QUEUE\tQUEUES")
		for _, p := range profiles {
			qRaw, _ := json.Marshal(p.DefaultQueues)
			fmt.Fprintf(tw, "%s\t%s\t%v\t%v\t%s\t%s\n",
				p.Kind, strings.Join(p.Aspects, ","), p.SupportsLiveState, p.SerialExecution, p.FetchStateQueue, string(qRaw))
		}
		return tw.Flush()

	case "get":
		if len(args) < 2 {
			return errors.New("usage: orchctl catalog get <kind>")
		}
		var p model.DeviceProfile
		if err := c.doJSON(http.MethodGet, "/catalog/profiles/"+args[1], nil, &p); err != nil {
			return err
		}
		return printJSON(p)

	case "put":
		if len(args) < 2 {
			return errors.New("usage: orchctl catalog put <kind> (--file=path | --json='{...}')")
		}
		kind := args[1]
		fs := flag.NewFlagSet("catalog put", flag.ExitOnError)
		filePath := fs.String("file", "", "path to profile JSON file")
		jsonStr := fs.String("json", "", "inline profile JSON")
		_ = fs.Parse(args[2:])

		var raw []byte
		var err error
		switch {
		case *filePath != "":
			raw, err = os.ReadFile(*filePath)
			if err != nil {
				return err
			}
		case *jsonStr != "":
			raw = []byte(*jsonStr)
		default:
			return errors.New("either --file or --json is required")
		}
		var p model.DeviceProfile
		if err := json.Unmarshal(raw, &p); err != nil {
			return fmt.Errorf("invalid profile json: %w", err)
		}
		var saved model.DeviceProfile
		if err := c.doJSON(http.MethodPut, "/catalog/profiles/"+kind, p, &saved); err != nil {
			return err
		}
		return printJSON(saved)

	case "delete":
		if len(args) < 2 {
			return errors.New("usage: orchctl catalog delete <kind>")
		}
		var resp map[string]any
		if err := c.doJSON(http.MethodDelete, "/catalog/profiles/"+args[1], nil, &resp); err != nil {
			return err
		}
		return printJSON(resp)

	case "reload":
		var resp map[string]any
		if err := c.doJSON(http.MethodPost, "/catalog/reload", nil, &resp); err != nil {
			return err
		}
		return printJSON(resp)

	default:
		return fmt.Errorf("unknown catalog subcommand %q", args[0])
	}
}

func (c *apiClient) listEntities() ([]*model.Entity, error) {
	var entities []*model.Entity
	if err := c.doJSON(http.MethodGet, "/entities", nil, &entities); err != nil {
		return nil, err
	}
	return entities, nil
}

func (c *apiClient) listPlans(entityID string) ([]*model.Plan, error) {
	var plans []*model.Plan
	if err := c.doJSON(http.MethodGet, "/entities/"+entityID+"/plans", nil, &plans); err != nil {
		return nil, err
	}
	return plans, nil
}

func (c *apiClient) doJSON(method, path string, reqBody, out any) error {
	var bodyReader io.Reader
	if reqBody != nil {
		raw, err := json.Marshal(reqBody)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequest(method, c.baseURL+path, bodyReader)
	if err != nil {
		return err
	}
	if reqBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("%s %s -> %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBytes)))
	}
	if out != nil && len(respBytes) > 0 {
		return json.Unmarshal(respBytes, out)
	}
	return nil
}

func renderEntities(w io.Writer, entities []*model.Entity, latestPlans map[string]*model.Plan) error {
	if len(entities) == 0 {
		_, err := fmt.Fprintln(w, "No entities registered.")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 2, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tKIND\tSTATUS\tVER\tCURRENT\tTARGET\tERROR")
	for _, e := range entities {
		curr, _ := json.Marshal(e.Current)
		tgt, _ := json.Marshal(e.Target)
		errCol := e.LastError
		if errCol == "" {
			errCol = "-"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%d\t%s\t%s\t%s\n",
			e.ID, e.Kind, e.Status, e.Version, string(curr), string(tgt), errCol)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if len(latestPlans) > 0 {
		fmt.Fprintln(w)
		for _, e := range entities {
			p := latestPlans[e.ID]
			if p == nil {
				continue
			}
			var steps []string
			for _, st := range p.Steps {
				steps = append(steps, fmt.Sprintf("%s:%s", st.ID, st.Status))
			}
			fmt.Fprintf(w, "  %s plan v%d (%s): %s\n", e.ID, p.Version, p.Status, strings.Join(steps, " | "))
		}
	}
	return nil
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
