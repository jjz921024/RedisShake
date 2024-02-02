package reader

import (
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
	"RedisShake/internal/status"
	"context"
	"errors"

	"github.com/mcuadros/go-defaults"
	"github.com/spf13/viper"
)

type Reader interface {
	status.Statusable
	StartRead(ctx context.Context) chan *entry.Entry
}

func CreateReader(v *viper.Viper) (Reader, error) {
	var theReader Reader
	if v.IsSet("sync_reader") {
		opts := new(SyncReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("sync_reader", opts)
		if err != nil {
			log.Panicf("failed to read the SyncReader config entry. err: %v", err)
		}
		if opts.Cluster {
			theReader = NewSyncClusterReader(opts)
			log.Infof("create SyncClusterReader: %v", opts.Address)
		} else {
			theReader = NewSyncStandaloneReader(opts)
			log.Infof("create SyncStandaloneReader: %v", opts.Address)
		}
	} else if v.IsSet("scan_reader") {
		opts := new(ScanReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("scan_reader", opts)
		if err != nil {
			log.Panicf("failed to read the ScanReader config entry. err: %v", err)
		}
		if opts.Cluster {
			theReader = NewScanClusterReader(opts)
			log.Infof("create ScanClusterReader: %v", opts.Address)
		} else {
			theReader = NewScanStandaloneReader(opts)
			log.Infof("create ScanStandaloneReader: %v", opts.Address)
		}
	} else if v.IsSet("rdb_reader") {
		opts := new(RdbReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("rdb_reader", opts)
		if err != nil {
			log.Panicf("failed to read the RdbReader config entry. err: %v", err)
		}
		theReader = NewRDBReader(opts)
		log.Infof("create RdbReader: %v", opts.Filepath)
	} else if v.IsSet("aof_reader") {
		opts := new(AOFReaderOptions)
		defaults.SetDefaults(opts)
		err := v.UnmarshalKey("aof_reader", opts)
		if err != nil {
			log.Panicf("failed to read the AOFReader config entry. err: %v", err)
		}
		theReader = NewAOFReader(opts)
		log.Infof("create AOFReader: %v", opts.Filepath)
	} else {
		return nil, errors.New("no reader config entry found")
	}
	return theReader, nil
}