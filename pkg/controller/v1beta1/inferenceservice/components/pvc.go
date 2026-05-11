package components

import (
	"github.com/ome-projects/ome/pkg/utils/storage"
)

// isPVCBaseModel reports whether b's referenced BaseModel is PVC-backed.
func isPVCBaseModel(b *BaseComponentFields) bool {
	if b == nil || b.BaseModel == nil || b.BaseModel.Storage == nil || b.BaseModel.Storage.StorageUri == nil {
		return false
	}
	t, err := storage.GetStorageType(*b.BaseModel.Storage.StorageUri)
	if err != nil {
		return false
	}
	return t == storage.StorageTypePVC
}

// parsePVCComponents returns the parsed PVC URI for b's BaseModel. Returns
// nil if the model is not PVC-backed or the URI is malformed; the caller
// is expected to have validated the model upstream (via the BaseModel
// reconciler / admission webhook), so a parse error here is treated as
// "no PVC" rather than fatal.
func parsePVCComponents(b *BaseComponentFields) *storage.PVCStorageComponents {
	if !isPVCBaseModel(b) {
		return nil
	}
	components, err := storage.ParsePVCStorageURI(*b.BaseModel.Storage.StorageUri)
	if err != nil {
		return nil
	}
	return components
}
