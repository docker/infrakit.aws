package metadata

import (
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/service/autoscaling/autoscalingiface"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/aws/aws-sdk-go/service/cloudformation/cloudformationiface"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/docker/infrakit.aws/plugin/instance"
	metadata_plugin "github.com/docker/infrakit/pkg/plugin/metadata"
	"github.com/docker/infrakit/pkg/spi/metadata"
	"github.com/docker/infrakit/pkg/template"
	"github.com/docker/infrakit/pkg/types"
)

type AWSClients struct {
	Cfn cloudformationiface.CloudFormationAPI
	Ec2 ec2iface.EC2API
	Asg autoscalingiface.AutoScalingAPI
}

type Context struct {
	update          chan func(map[string]interface{})
	poll            time.Duration
	templateURL     string
	templateOptions template.Options
	stop            <-chan struct{}
	stackName       string // cloudformation stackname
	clients         AWSClients
	impl            metadata.Plugin
}

func (c *Context) start() {

	update := make(chan func(map[string]interface{}))
	tick := time.Tick(c.poll)

	c.impl = metadata_plugin.NewPluginFromChannel(update)
	c.update = update

	go func() {

	loop:
		for {
			select {
			case <-tick:

				t, err := template.NewTemplate(c.templateURL, c.templateOptions)
				if err != nil {
					update <- func(view map[string]interface{}) {
						view["err"] = err
					}
					continue loop
				}

				// Note the actual exporting of the values is done via the 'export' function
				// that are invoked as part of processing the template.
				_, err = t.Render(c)
				if err != nil {
					update <- func(view map[string]interface{}) {
						view["err"] = err
					}
					continue loop
				}

			case <-c.stop:
				log.Infoln("Stopping aws metadata")
				close(update)
				return
			}
		}

	}()
}

// List returns a list of *child nodes* given a path, which is specified as a slice
// where for i > j path[i] is the parent of path[j]
func (c *Context) List(path metadata.Path) (child []string, err error) {
	return c.impl.List(path)
}

// Get retrieves the value at path given.
func (c *Context) Get(path metadata.Path) (value *types.Any, err error) {
	return c.impl.Get(path)
}

// Funcs return the additional functions that are available for AWS.
func (c *Context) Funcs() []template.Function {
	return []template.Function{
		{
			Name: "export",
			Description: []string{
				"export makes the value (second argument) available as metadata at path (first arg).",
			},
			Func: func(p string, value interface{}) (string, error) {
				if c.update == nil {
					return "", fmt.Errorf("cannot export")
				}

				c.update <- func(view map[string]interface{}) {
					metadata_plugin.Put([]string(metadata_plugin.Path(p)), value, view)
				}

				return "", nil
			},
		},
		{
			Name: "describe",
			Description: []string{
				"Describe takes the input path (arg1) and applies the query on the second parameter (the result of the 'cfn')",
				"and calls the describe via the AWS API. Currently only a few resource types are supported.",
			},
			Func: func(p string, obj interface{}) (interface{}, error) {
				if obj == nil {
					return nil, nil
				}
				o, err := template.QueryObject(p, obj)
				if err != nil {
					return nil, err
				}

				switch o := o.(type) {

				case *cloudformation.StackResource:
					return describe(c.clients, o)

				case map[string]interface{}:
					rr := &cloudformation.StackResource{}
					err := template.FromMap(o, rr)
					if err != nil {
						return nil, err
					}
					return describe(c.clients, rr)
				}
				return nil, fmt.Errorf("unknown object:", o)
			},
		},
		{
			Name: "cfn",
			Description: []string{
				"cfn takes a string that is the stack name and retrieves the cloudformation data of the stack.",
			},
			Func: func(p string) (interface{}, error) {
				return cfn(c.clients, p)
			},
		},
		{
			Name: "region",
			Description: []string{
				"region returns the AWS region using metdata lookup",
			},
			Func: func() (string, error) {
				return instance.GetRegion()
			},
		},
		{
			Name: "stackName",
			Description: []string{
				"stackName returns the stack name (for cloudformation) if specified.",
			},
			Func: func() string {
				return c.stackName
			},
		},
	}
}
