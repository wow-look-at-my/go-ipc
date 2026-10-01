package gen

import (
	"encoding/json"

	"github.com/wow-look-at-my/go-ipc/ipcgen/internal/schema"
)

// jsonType describes a type. Kind is a scalar name, "string", "bytes", "array" or "message".
type jsonType struct {
	Kind    string    `json:"kind"`
	Len     int       `json:"len,omitempty"`
	Elem    *jsonType `json:"elem,omitempty"`
	Message string    `json:"message,omitempty"`
}

type jsonField struct {
	Name   string    `json:"name"`
	Type   *jsonType `json:"type"`
	Offset *int      `json:"offset,omitempty"`
	Size   *int      `json:"size,omitempty"`
}

type jsonMessage struct {
	Name      string       `json:"name"`
	Snake     string       `json:"snake"`
	ID        uint32       `json:"id"`
	FixedSize int          `json:"fixed_size"`
	Align     int          `json:"align"`
	Fields    []*jsonField `json:"fields"`
}

type jsonSchema struct {
	Comment  string         `json:"comment"`
	Package  string         `json:"package"`
	Messages []*jsonMessage `json:"messages"`
}

func toJSONType(t *schema.Type) *jsonType {
	switch t.Kind {
	case schema.Array:
		return &jsonType{Kind: "array", Len: t.Len, Elem: toJSONType(t.Elem)}
	case schema.Ref:
		return &jsonType{Kind: "message", Message: t.Name}
	}
	return &jsonType{Kind: t.Kind.String()}
}

// genJSON describes the checked schema and its layout as JSON, for tools and tests.
func genJSON(s *schema.Schema) ([]byte, error) {
	out := jsonSchema{Comment: Header, Package: s.Package, Messages: []*jsonMessage{}}
	for _, m := range s.Messages {
		jm := &jsonMessage{Name: m.Name, Snake: m.Snake(), ID: m.ID, FixedSize: m.FixedSize, Align: m.Align, Fields: []*jsonField{}}
		for _, f := range m.Fields {
			jf := &jsonField{Name: f.Name, Type: toJSONType(f.Type)}
			if !f.IsVar() {
				off, size := f.Offset, f.Type.Size()
				jf.Offset, jf.Size = &off, &size
			}
			jm.Fields = append(jm.Fields, jf)
		}
		out.Messages = append(out.Messages, jm)
	}
	b, err := json.MarshalIndent(out, "", "\t")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
