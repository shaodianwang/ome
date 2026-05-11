package enigma

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"github.com/ome-projects/ome/pkg/logging"
	"github.com/ome-projects/ome/pkg/vault/kmscrypto"
	"github.com/ome-projects/ome/pkg/vault/kmsmgm"
	ocisecret "github.com/ome-projects/ome/pkg/vault/secret"
)

type enigmaParams struct {
	fx.In

	AnotherLogger   logging.Interface `name:"another_log"`
	KmsCryptoClient *kmscrypto.KmsCrypto
	KmsManagement   *kmsmgm.KmsMgm
	Secret          *ocisecret.Secret
}

var Module = fx.Provide(
	func(v *viper.Viper, params enigmaParams) (*Enigma, error) {
		config, err := NewConfig(
			WithViper(v, params.AnotherLogger),
			WithAppParams(params),
			WithAnotherLog(params.AnotherLogger),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating enigma config: %+v", err)
		}
		return NewApplication(config)
	})
