package controller

import (
	"net"
	"strconv"

	"github.com/phayes/freeport"
	"github.com/rs/zerolog/log"
)

var controllerLogger = log.With().Str("component", "controller").Logger()

type SuplementaryServer struct {
	metricsPort                int
	metricsListener            net.Listener
	suplementaryServerPort     int
	suplementaryServerListener net.Listener
}

func NewSuplementServer(metricPort int) (SuplementaryServer, error) {
	freePort, err := freeport.GetFreePort()
	if err != nil {
		controllerLogger.Error().Err(err).Msg("Failed to get free port")
	} else {
		controllerLogger.Info().Msgf("Got free port: %v", freePort)
	}

	metricListener, err := net.Listen("tcp", ":"+strconv.Itoa(metricPort))
	if err != nil {
		controllerLogger.Error().Err(err).Msgf("Failed to listen on %v", metricPort)
		return SuplementaryServer{}, err
	} else {
		defer metricListener.Close()
	}

	suplementaryServerListener, err := net.Listen("tcp", ":"+strconv.Itoa(freePort))
	if err != nil {
		controllerLogger.Error().Err(err).Msgf("Failed to listen on %v", freePort)
		return SuplementaryServer{}, err
	} else {
		defer suplementaryServerListener.Close()
	}

	return SuplementaryServer{
		metricsPort:                metricPort,
		metricsListener:            metricListener,
		suplementaryServerPort:     freePort,
		suplementaryServerListener: suplementaryServerListener,
	}, nil
}

// func (s *SuplementaryServer) Start(ctx context.Context) error {
