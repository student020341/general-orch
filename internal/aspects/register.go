package aspects

import (
	"orch/internal/aspects/light"
	"orch/internal/aspects/rotation"
	"orch/internal/model"
	"orch/internal/reconciler"
)

func all() []reconciler.AspectPlanner {
	return []reconciler.AspectPlanner{
		light.New(),
		rotation.New(),
	}
}

// RegisterAll registers all built-in capability aspect planners onto r.
func RegisterAll(r *reconciler.Reconciler) {
	for _, ap := range all() {
		r.RegisterAspect(ap)
	}
}

// Schemas returns contract schemas for all built-in aspect types.
func Schemas() []model.AspectSchema {
	list := all()
	out := make([]model.AspectSchema, 0, len(list))
	for _, ap := range list {
		out = append(out, ap.Schema())
	}
	return out
}
