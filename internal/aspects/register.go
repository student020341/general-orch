package aspects

import (
	"orch/internal/aspects/lights"
	"orch/internal/aspects/rotation"
	"orch/internal/reconciler"
)

// RegisterAll registers all built-in capability aspect planners onto r.
func RegisterAll(r *reconciler.Reconciler) {
	r.RegisterAspect(lights.New())
	r.RegisterAspect(rotation.New())
}
