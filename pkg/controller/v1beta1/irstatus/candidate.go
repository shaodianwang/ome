package irstatus

import (
	"encoding/json"

	"github.com/ome-projects/ome/pkg/apis/ome/v1beta1"
)

// Candidate is one complete status representation ready to be written.
type Candidate struct {
	Encoding Encoding
	// Status is the write copy carrying exactly this representation.
	Status *v1beta1.InferenceReplicaStatus
	// Size is the serialized JSON length of Status. The candidates differ
	// only in the per-Instance representation, so comparing status bytes
	// orders complete update requests identically.
	Size int
}

// Selection is the outcome of comparing both representations of one
// logical status.
type Selection struct {
	// DenseV1 is always built.
	DenseV1 Candidate
	// ColumnarV2 is nil when the rows are not eligible for columns;
	// IneligibleReason then names why, and DenseV1 is selected.
	ColumnarV2       *Candidate
	IneligibleReason ErrorReason
	// Selected is ColumnarV2 only when its Size is strictly smaller than
	// the DenseV1 Size; a tie selects DenseV1.
	Selected Encoding
}

// SelectedCandidate returns the candidate to write.
func (s *Selection) SelectedCandidate() *Candidate {
	if s.Selected == EncodingColumnarV2 && s.ColumnarV2 != nil {
		return s.ColumnarV2
	}
	return &s.DenseV1
}

// SelectCandidate builds both complete status candidates for logical, a
// status in the DenseV1 logical shape (rows in instanceStatuses, no marker,
// no columns), and selects the strictly smaller serialization. The three
// Pod-derived row fields are cleared on the write copies before either
// candidate is built, so neither the comparison nor the written object
// carries them. Equal input always yields the same selection. The two
// candidates share every status field outside the per-Instance
// representation; the caller writes one of them and discards the other.
func SelectCandidate(logical *v1beta1.InferenceReplicaStatus, maxInstances uint64) (*Selection, error) {
	if logical == nil || logical.InstanceStatusEncoding != nil || logical.InstanceStatusColumns != nil {
		return nil, newCodecError(ErrorReasonRepresentationUnion)
	}

	dense := logical.DeepCopy()
	ClearPodDerivedObservations(dense.InstanceStatuses)
	denseSize, err := serializedSize(dense)
	if err != nil {
		return nil, err
	}
	selection := &Selection{
		DenseV1:  Candidate{Encoding: EncodingDenseV1, Status: dense, Size: denseSize},
		Selected: EncodingDenseV1,
	}

	columns, err := EncodeColumns(dense.InstanceStatuses, maxInstances)
	if err != nil {
		reason, ok := ErrorReasonOf(err)
		if !ok {
			return nil, err
		}
		selection.IneligibleReason = reason
		return selection, nil
	}
	columnar := *dense
	columnar.InstanceStatuses = nil
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	columnar.InstanceStatusEncoding = &encoding
	columnar.InstanceStatusColumns = columns
	columnarSize, err := serializedSize(&columnar)
	if err != nil {
		return nil, err
	}
	selection.ColumnarV2 = &Candidate{Encoding: EncodingColumnarV2, Status: &columnar, Size: columnarSize}
	selection.Selected = selectEncoding(denseSize, columnarSize)
	return selection, nil
}

// selectEncoding applies the strict-smaller rule: columns win only when they
// are smaller; a tie keeps the dense list.
func selectEncoding(denseSize, columnarSize int) Encoding {
	if columnarSize < denseSize {
		return EncodingColumnarV2
	}
	return EncodingDenseV1
}

func serializedSize(status *v1beta1.InferenceReplicaStatus) (int, error) {
	data, err := json.Marshal(status)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}
