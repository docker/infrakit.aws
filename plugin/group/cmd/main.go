package main

import (
	"encoding/json"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/docker/go-connections/tlsconfig"
	"github.com/docker/libmachete.aws"
	"github.com/docker/libmachete.aws/plugin/group"
	group_plugin "github.com/docker/libmachete/plugin/group"
	"github.com/docker/libmachete/plugin/group/swarm"
	"github.com/docker/libmachete/spi/instance"
	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"os"
	"time"
)

var (
	logLevel = len(log.AllLevels) - 2

	listen = "unix:///run/docker/plugins/group.sock"

	// PluginName is the name of the plugin in the Docker Hub / registry
	PluginName = "NoPluginName"

	// PluginType is the name of the container image name / plugin name
	PluginType = "docker.groupDriver/1.0"

	// PluginNamespace is the namespace of the plugin
	PluginNamespace = "/aws/group"

	// Version is the build release identifier.
	Version = "Unspecified"

	// Revision is the build source control revision.
	Revision = "Unspecified"
)

const (
	// Default host value borrowed from github.com/docker/docker/opts
	defaultHost = "unix:///var/run/docker.sock"
)

// Docker login options
var (
	tlsOptions = tlsconfig.Options{}
	host       = defaultHost
)

func info() interface{} {
	return map[string]interface{}{
		"name":      PluginName,
		"type":      PluginType,
		"namespace": PluginNamespace,
		"version":   Version,
		"revision":  Revision,
	}
}

func main() {

	builder := &aws.Builder{}

	cmd := &cobra.Command{
		Use:   "group",
		Short: "Group plugin for managing groups",
		RunE: func(c *cobra.Command, args []string) error {

			if logLevel > len(log.AllLevels)-1 {
				logLevel = len(log.AllLevels) - 1
			} else if logLevel < 0 {
				logLevel = 0
			}
			log.SetLevel(log.AllLevels[logLevel])

			if c.Use == "version" {
				return nil
			}

			log.Infoln("Connecting to Docker:", host, tlsOptions)

			dockerClient, err := group.NewDockerClient(host, &tlsOptions)
			if err != nil {
				log.Error(err)
				return err
			}

			// TODO - This is static and only includes the AWS instance plugin for now.
			// Next - Implement instance plugin RPC clients and discovery of the plugins of type 'instanceDriver'.

			provisioner, err := builder.BuildInstancePlugin()
			if err != nil {
				log.Error(err)
				return err
			}

			grp := group_plugin.NewGroupPlugin(
				func(k string) (instance.Plugin, error) {
					if k == "aws" {
						return provisioner, nil
					}
					return nil, nil
				},
				swarm.NewSwarmProvisionHelper(dockerClient),
				1*time.Second)

			adapter := httpAdapter{plugin: grp}

			router := mux.NewRouter()
			router.StrictSlash(true)

			router.HandleFunc("/Watch", outputHandler(adapter.watch)).Methods("POST")
			router.HandleFunc("/Unwatch/{id}", outputHandler(adapter.unwatch)).Methods("POST")
			router.HandleFunc("/Inspect/{id}", outputHandler(adapter.inspect)).Methods("POST")
			router.HandleFunc("/DescribeUpdate", outputHandler(adapter.describeUpdate)).Methods("POST")
			router.HandleFunc("/UpdateGroup", outputHandler(adapter.updateGroup)).Methods("POST")
			router.HandleFunc("/DestroyGroup/{id}", outputHandler(adapter.destroyGroup)).Methods("POST")

			log.Infoln("Starting httpd")
			log.Infoln("Listening on:", listen)

			_, waitHTTP, err := group.StartServer(listen, router,
				func() error {
					log.Infoln("Shutting down.")
					return nil
				})
			if err != nil {
				panic(err)
			}
			log.Infoln("Started httpd")

			<-waitHTTP
			return nil
		},
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "version",
		Short: "print build version information",
		RunE: func(cmd *cobra.Command, args []string) error {
			buff, err := json.MarshalIndent(info(), "  ", "  ")
			if err != nil {
				return err
			}
			fmt.Println(string(buff))
			return nil
		},
	})

	cmd.PersistentFlags().StringVar(&listen, "listen", listen, "listen address (unix or tcp)")
	cmd.PersistentFlags().IntVar(&logLevel, "log", logLevel, "Logging level. 0 is least verbose. Max is 5")

	cmd.PersistentFlags().StringVar(&host, "host", defaultHost, "Docker host")
	cmd.PersistentFlags().StringVar(&tlsOptions.CAFile, "tlscacert", "", "TLS CA cert")
	cmd.PersistentFlags().StringVar(&tlsOptions.CertFile, "tlscert", "", "TLS cert")
	cmd.PersistentFlags().StringVar(&tlsOptions.KeyFile, "tlskey", "", "TLS key")
	cmd.PersistentFlags().BoolVar(&tlsOptions.InsecureSkipVerify, "tlsverify", true, "True to skip TLS")

	// TODO(chungers) - the exposed flags here won't be set in plugins, because plugin install doesn't allow
	// user to pass in command line args like containers with entrypoint.
	cmd.Flags().AddFlagSet(builder.Flags())

	err := cmd.Execute()
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}
}
