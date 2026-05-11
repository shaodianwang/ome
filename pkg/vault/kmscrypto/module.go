package kmscrypto

import (
	"fmt"

	"github.com/spf13/viper"
	"go.uber.org/fx"

	"github.com/ome-projects/ome/pkg/logging"
	"github.com/ome-projects/ome/pkg/vault/kmsvault"
)

type kmsCryptoParams struct {
	fx.In

	AnotherLogger  logging.Interface `name:"another_log"`
	KmsVaultClient *kmsvault.KMSVault
}

var Module = fx.Provide(
	func(v *viper.Viper, params kmsCryptoParams) (*KmsCrypto, error) {
		config, err := NewConfig(
			WithViper(v, params.AnotherLogger),
			WithAppParams(params),
			WithAnotherLog(params.AnotherLogger),
		)
		if err != nil {
			return nil, fmt.Errorf("error creating kms crypto config: %+v", err)
		}
		return NewKmsCrypto(config)
	})
