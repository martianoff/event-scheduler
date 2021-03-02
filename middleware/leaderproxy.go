package middleware

import (
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	nativemiddleware "github.com/labstack/echo/v4/middleware"
	"github.com/maksimru/event-scheduler/config"
	log "github.com/sirupsen/logrus"
	"net/url"
	"strings"
)

type LeaderProxy struct {
	cluster *raft.Raft
	config  config.Config
}

func NewLeaderProxy(cluster *raft.Raft, config config.Config) *LeaderProxy {
	return &LeaderProxy{
		cluster: cluster,
		config:  config,
	}
}

func (l *LeaderProxy) Process(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		// skip middleware for leader node
		if l.cluster.State() == raft.Leader {
			return next(c)
		}
		// transform leader endpoint to api endpoint
		apiEndpoint := strings.Replace(string(l.cluster.Leader()), ":"+l.config.ClusterNodePort, ":"+l.config.APIPort, 1)
		leaderAddr, err := url.Parse("http://" + apiEndpoint)
		if err != nil {
			log.Error("http middleware: error during leader lookup", apiEndpoint)
			return err
		}
		log.Trace("proxying API request to the leader at", apiEndpoint)
		return nativemiddleware.Proxy(nativemiddleware.NewRoundRobinBalancer(
			[]*nativemiddleware.ProxyTarget{
				{
					Name: "Leader",
					URL:  leaderAddr,
				},
			},
		))(next)(c)
	}
}
