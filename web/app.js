const state = {
  selectedEntityId: null,
  selectedMissingKind: "",
  profiles: [],
  entities: [],
};

async function api(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers["Content-Type"] = "application/json";
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(path, opts);
  const text = await res.text();
  const data = text ? JSON.parse(text) : null;
  if (!res.ok) {
    throw new Error((data && data.error) || `${method} ${path} -> ${res.status}`);
  }
  return data;
}

function pill(status) {
  const safe = String(status || "unknown").toLowerCase();
  return `<span class="pill pill-${safe}">${escapeHtml(safe)}</span>`;
}

function escapeHtml(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function renderVisual(curr) {
  if (!curr) return "-";
  const parts = [];
  const lightKeys = Object.keys(curr)
    .filter((k) => k === "light" || (k.startsWith("light") && k !== "lights"))
    .sort();
  for (const key of lightKeys) {
    const c = curr[key];
    if (c && typeof c === "object" && "r" in c && "g" in c && "b" in c) {
      parts.push(
        `<span class="swatch" title="${escapeHtml(key)}: rgb(${c.r},${c.g},${c.b})" style="background:rgb(${c.r},${c.g},${c.b})"></span>`
      );
    }
  }
  if (typeof curr.rotation === "number") {
    parts.push(`<code>${curr.rotation}°</code>`);
  }
  return parts.length ? `<div class="visual-row">${parts.join("")}</div>` : "-";
}

async function refreshHealth() {
  const el = document.getElementById("health-pill");
  try {
    await api("GET", "/healthz");
    el.textContent = "gateway online";
    el.className = "pill pill-ok";
  } catch {
    el.textContent = "offline";
    el.className = "pill pill-failed";
  }
}

async function refreshEntities() {
  const entities = (await api("GET", "/entities")) || [];
  state.entities = entities;
  document.getElementById("entity-count").textContent = `${entities.length} entities`;

  const tbody = document.getElementById("entities-body");
  if (!entities.length) {
    tbody.innerHTML = `<tr><td colspan="6" class="muted">No entities yet. Submit an intent to get started.</td></tr>`;
    return;
  }

  tbody.innerHTML = entities
    .map((e) => {
      const selected = e.id === state.selectedEntityId ? "selected" : "";
      const errLine = e.last_error ? `<div class="muted" style="color:var(--err)">${escapeHtml(e.last_error)}</div>` : "";
      const hasProfile = state.profiles.some((p) => p.kind === e.kind);
      const kindBadge = hasProfile ? "" : ` <span class="pill pill-superseded" title="No matching device profile in catalog">no profile</span>`;
      return `
        <tr class="clickable ${selected}" data-id="${escapeHtml(e.id)}">
          <td><strong>${escapeHtml(e.id)}</strong></td>
          <td>${escapeHtml(e.kind)}${kindBadge}<div class="muted">${escapeHtml(e.vendor)}${e.task_queue ? " · " + escapeHtml(e.task_queue) : ""}</div></td>
          <td>${pill(e.status)}${errLine}</td>
          <td>v${e.version}</td>
          <td>${renderVisual(e.current)}</td>
          <td class="diff-cell">
            <code>curr: ${escapeHtml(JSON.stringify(e.current))}</code>
            <code>tgt:  ${escapeHtml(JSON.stringify(e.target))}</code>
          </td>
        </tr>
      `;
    })
    .join("");

  tbody.querySelectorAll("tr[data-id]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedEntityId = row.dataset.id;
      document.getElementById("intent-id").value = row.dataset.id;
      const entity = state.entities.find((x) => x.id === row.dataset.id);
      if (entity) {
        const hasProfile = state.profiles.some((p) => p.kind === entity.kind);
        state.selectedMissingKind = hasProfile ? "" : entity.kind;
        syncKindSelect(entity.kind);
        document.getElementById("intent-vendor").value = entity.vendor;
      }
      refreshEntities();
      refreshSelectedDetails();
    });
  });
}

function syncKindSelect(desiredKind) {
  const select = document.getElementById("intent-kind");
  const prev = desiredKind !== undefined ? desiredKind : select.value;

  const options = state.profiles.map((p) => ({
    value: p.kind,
    label: p.kind,
  }));
  if (state.selectedMissingKind && !state.profiles.some((p) => p.kind === state.selectedMissingKind)) {
    options.push({
      value: state.selectedMissingKind,
      label: `${state.selectedMissingKind} (no profile)`,
    });
  }

  const signature = options.map((o) => `${o.value}:${o.label}`).join("|");
  if (select.dataset.signature !== signature) {
    select.innerHTML = options
      .map((o) => `<option value="${escapeHtml(o.value)}">${escapeHtml(o.label)}</option>`)
      .join("");
    select.dataset.signature = signature;
  }

  if (prev && options.some((o) => o.value === prev)) {
    select.value = prev;
  }
}

async function refreshSelectedDetails() {
  const id = state.selectedEntityId;
  const titleEl = document.getElementById("plans-title");
  const plansEl = document.getElementById("plans-list");
  const mockBox = document.getElementById("mock-box");

  if (!id) {
    titleEl.textContent = "Execution Plans";
    mockBox.classList.add("hidden");
    return;
  }

  titleEl.textContent = `Execution Plans — ${id}`;

  // Check optional cmd/dev mock hardware endpoint.
  try {
    const mockState = await api("GET", `/_dev/mock/${encodeURIComponent(id)}`);
    document.getElementById("mock-state").textContent = JSON.stringify(mockState);
    mockBox.classList.remove("hidden");
  } catch {
    mockBox.classList.add("hidden");
  }

  const plans = (await api("GET", `/entities/${encodeURIComponent(id)}/plans`)) || [];
  if (!plans.length) {
    plansEl.innerHTML = `<p class="muted">No plans recorded for ${escapeHtml(id)}.</p>`;
    return;
  }

  plansEl.innerHTML = plans
    .map((p) => {
      const stepsHtml = (p.steps || [])
        .map((st) => {
          const deps = st.depends_on && st.depends_on.length ? `deps: [${st.depends_on.join(", ")}]` : "root";
          const err = st.error ? `<span style="color:var(--err)">${escapeHtml(st.error)}</span>` : "";
          return `
            <div class="step-row">
              <strong>${escapeHtml(st.id)}</strong>
              ${pill(st.status)}
              <code>${escapeHtml(st.task_queue)}</code>
              <span class="muted">${escapeHtml(st.capability)} · ${escapeHtml(deps)}</span>
              <code>${escapeHtml(JSON.stringify(st.payload))}</code>
              ${err}
            </div>
          `;
        })
        .join("");
      return `
        <div class="plan-card">
          <div class="plan-head">
            <div><strong>v${p.version}</strong> <code class="muted">${escapeHtml(p.id)}</code></div>
            ${pill(p.status)}
          </div>
          ${stepsHtml}
        </div>
      `;
    })
    .join("");
}

async function refreshCatalog() {
  const profiles = (await api("GET", "/catalog/profiles")) || [];
  state.profiles = profiles;

  if (state.selectedMissingKind && profiles.some((p) => p.kind === state.selectedMissingKind)) {
    state.selectedMissingKind = "";
  }
  syncKindSelect();

  const tbody = document.getElementById("catalog-body");
  tbody.innerHTML = profiles
    .map((p) => {
      return `
        <tr>
          <td><strong>${escapeHtml(p.kind)}</strong></td>
          <td><code>${escapeHtml((p.aspects || []).join(", "))}</code></td>
          <td>${p.supports_live_state ? "yes" : "no"}</td>
          <td>${p.serial_execution ? "serial" : "parallel"}</td>
          <td><code>${escapeHtml(JSON.stringify(p.default_queues || {}))}</code></td>
          <td class="btn-group">
            <button type="button" class="btn btn-xs edit-profile" data-kind="${escapeHtml(p.kind)}">Edit</button>
            <button type="button" class="btn btn-xs btn-danger del-profile" data-kind="${escapeHtml(p.kind)}">Delete</button>
          </td>
        </tr>
      `;
    })
    .join("");

  tbody.querySelectorAll(".edit-profile").forEach((btn) => {
    btn.addEventListener("click", () => {
      const p = state.profiles.find((x) => x.kind === btn.dataset.kind);
      if (!p) return;
      document.getElementById("profile-kind").value = p.kind;
      document.getElementById("profile-fetch-queue").value = p.fetch_state_queue || "";
      document.getElementById("profile-json").value = JSON.stringify(
        {
          aspects: p.aspects,
          default_queues: p.default_queues,
          fetch_state_queue: p.fetch_state_queue,
          supports_live_state: p.supports_live_state,
          serial_execution: p.serial_execution,
        },
        null,
        2
      );
    });
  });

  tbody.querySelectorAll(".del-profile").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const kind = btn.dataset.kind;
      const msg = document.getElementById("catalog-msg");
      try {
        await api("DELETE", `/catalog/profiles/${encodeURIComponent(kind)}`);
        msg.textContent = `Deleted ${kind}`;
        await refreshCatalog();
      } catch (err) {
        msg.textContent = err.message;
      }
    });
  });
}

async function refreshAll() {
  await Promise.all([refreshHealth(), refreshCatalog()]);
  await refreshEntities();
  await refreshSelectedDetails();
}

document.getElementById("intent-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const msg = document.getElementById("intent-msg");
  try {
    const id = document.getElementById("intent-id").value.trim();
    const kind = document.getElementById("intent-kind").value.trim();
    const vendor = document.getElementById("intent-vendor").value.trim();
    const taskQueue = document.getElementById("intent-queue").value.trim();
    const patch = JSON.parse(document.getElementById("intent-patch").value);
    const replace = document.getElementById("intent-replace").checked;

    await api("PUT", `/entities/${encodeURIComponent(id)}/intent`, {
      kind,
      vendor,
      task_queue: taskQueue,
      patch,
      replace,
    });
    state.selectedEntityId = id;
    msg.textContent = `Submitted ${id}`;
    setTimeout(refreshAll, 250);
  } catch (err) {
    msg.textContent = err.message;
  }
});

document.getElementById("mock-drift-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  if (!state.selectedEntityId) return;
  try {
    const patch = JSON.parse(document.getElementById("mock-drift-input").value);
    await api("POST", `/_dev/mock/${encodeURIComponent(state.selectedEntityId)}`, patch);
    await refreshSelectedDetails();
  } catch (err) {
    alert(err.message);
  }
});

document.getElementById("catalog-form").addEventListener("submit", async (ev) => {
  ev.preventDefault();
  const msg = document.getElementById("profile-msg");
  try {
    const kind = document.getElementById("profile-kind").value.trim();
    const body = JSON.parse(document.getElementById("profile-json").value);
    const fetchQueue = document.getElementById("profile-fetch-queue").value.trim();
    if (fetchQueue && !body.fetch_state_queue) {
      body.fetch_state_queue = fetchQueue;
    }
    await api("PUT", `/catalog/profiles/${encodeURIComponent(kind)}`, body);
    msg.textContent = `Saved ${kind} & broadcast reload`;
    await refreshCatalog();
  } catch (err) {
    msg.textContent = err.message;
  }
});

document.getElementById("reload-catalog-btn").addEventListener("click", async () => {
  const msg = document.getElementById("catalog-msg");
  try {
    const res = await api("POST", "/catalog/reload");
    msg.textContent = `Reloaded ${res.count} profiles from ${res.source}`;
    await refreshCatalog();
  } catch (err) {
    msg.textContent = err.message;
  }
});

document.getElementById("preset-lamp").addEventListener("click", () => {
  const rand = () => Math.floor(Math.random() * 256);
  document.getElementById("intent-patch").value = JSON.stringify(
    {
      light: { r: rand(), g: rand(), b: rand() },
    },
    null,
    2
  );
});

document.getElementById("preset-beacon").addEventListener("click", () => {
  const rand = () => Math.floor(Math.random() * 256);
  const rot = Math.floor(Math.random() * 360);
  document.getElementById("intent-patch").value = JSON.stringify(
    {
      light1: { r: rand(), g: rand(), b: rand() },
      light2: { r: rand(), g: rand(), b: rand() },
      rotation: rot,
    },
    null,
    2
  );
});

document.getElementById("preset-turn").addEventListener("click", () => {
  document.getElementById("intent-patch").value = JSON.stringify({ rotation: 10 }, null, 2);
});

document.getElementById("refresh-btn").addEventListener("click", refreshAll);

setInterval(() => {
  if (document.getElementById("auto-refresh").checked) {
    refreshAll();
  }
}, 1500);

refreshAll();
