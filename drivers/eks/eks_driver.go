package eks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/eks"
	"github.com/aws/aws-sdk-go/service/iam"
	heptio "github.com/heptio/authenticator/pkg/token"
	"github.com/rancher/kontainer-engine/drivers/options"
	"github.com/rancher/kontainer-engine/drivers/util"
	"github.com/rancher/kontainer-engine/types"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	amiNamePrefix = "amazon-eks-node-"
	batchSize = 5 // maximum threads could be launched for creation multiple instance
)

var amiForRegionAndVersion = map[string]map[string]string{
	"1.11": map[string]string{
		"us-west-2":      "ami-0a2abab4107669c1b",
		"us-east-1":      "ami-0c24db5df6badc35a",
		"us-east-2":      "ami-0c2e8d28b1f854c68",
		"eu-central-1":   "ami-010caa98bae9a09e2",
		"eu-north-1":     "ami-06ee67302ab7cf838",
		"eu-west-1":      "ami-01e08d22b9439c15a",
		"ap-northeast-1": "ami-0f0e8066383e7a2cb",
		"ap-northeast-2": "ami-0b7baa90de70f683f",
		"ap-southeast-1": "ami-019966ed970c18502",
		"ap-southeast-2": "ami-06ade0abbd8eca425",
	},
	"1.10": map[string]string{
		"us-west-2":      "ami-09e1df3bad220af0b",
		"us-east-1":      "ami-04358410d28eaab63",
		"us-east-2":      "ami-0b779e8ab57655b4b",
		"eu-central-1":   "ami-08eb700778f03ea94",
		"eu-north-1":     "ami-068b8a1efffd30eda",
		"eu-west-1":      "ami-0de10c614955da932",
		"ap-northeast-1": "ami-06398bdd37d76571d",
		"ap-northeast-2": "ami-08a87e0a7c32fa649",
		"ap-southeast-1": "ami-0ac3510e44b5bf8ef",
		"ap-southeast-2": "ami-0d2c929ace88cfebe",
	},
}

type Driver struct {
	types.UnimplementedClusterSizeAccess
	types.UnimplementedVersionAccess

	driverCapabilities types.Capabilities

	request.Retryer
	metadata.ClientInfo

	Config   aws.Config
	Handlers request.Handlers
}

type State struct {
	ClusterName  string
	DisplayName  string
	ClientID     string
	ClientSecret string
	SessionToken string

	KubernetesVersion string

	UserData string
	Region   string

	VirtualNetwork              string
	Subnets                     []string
	SecurityGroups              []string
	ServiceRole                 string
	AMI                         string
	AssociateWorkerNodePublicIP *bool

	IngressRules []string
	EgressRules  []string

	Tags map[string]string

	KeyPairName string

	ClusterInfo types.ClusterInfo

	InstanceGroups InstanceGroups
}
type InstanceGroups []InstanceGroup

type InstanceGroup struct {
	InstanceGroupName string
	InstanceType      string
	InstanceCount     int64
	MinimumASGSize    int64
	MaximumASGSize    int64
	NodeVolumeSize    *int64
	Labels            map[string]string
}

type securityGroupRule struct {
	protocol string
	fromPort uint16
	toPort   uint16
	cidrIP   string
}

func NewDriver() types.Driver {
	driver := &Driver{
		driverCapabilities: types.Capabilities{
			Capabilities: make(map[int64]bool),
		},
	}
	driver.driverCapabilities.AddCapability(types.GetVersionCapability)
	driver.driverCapabilities.AddCapability(types.SetVersionCapability)

	return driver
}

func (d *Driver) GetDriverCreateOptions(ctx context.Context) (*types.DriverFlags, error) {
	driverFlag := types.DriverFlags{
		Options: make(map[string]*types.Flag),
	}
	driverFlag.Options["display-name"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The displayed name of the cluster in the Rancher UI",
	}
	driverFlag.Options["access-key"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The AWS Client ID to use",
	}
	driverFlag.Options["secret-key"] = &types.Flag{
		Type:     types.StringType,
		Password: true,
		Usage:    "The AWS Client Secret associated with the Client ID",
	}
	driverFlag.Options["session-token"] = &types.Flag{
		Type:  types.StringType,
		Usage: "A session token to use with the client key and secret if applicable.",
	}
	driverFlag.Options["region"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The AWS Region to create the EKS cluster in",
		Default: &types.Default{
			DefaultString: "us-west-2",
		},
	}
	driverFlag.Options["instance-type"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The type of machine to use for worker nodes",
		Default: &types.Default{
			DefaultString: "t2.medium",
		},
	}
	driverFlag.Options["instance-count"] = &types.Flag{
		Type:  types.IntType,
		Usage: "The initially instance count of worker nodes",
		Default: &types.Default{
			DefaultInt: 2,
		},
	}
	driverFlag.Options["minimum-nodes"] = &types.Flag{
		Type:  types.IntType,
		Usage: "The minimum number of worker nodes",
		Default: &types.Default{
			DefaultInt: 1,
		},
	}
	driverFlag.Options["maximum-nodes"] = &types.Flag{
		Type:  types.IntType,
		Usage: "The maximum number of worker nodes",
		Default: &types.Default{
			DefaultInt: 3,
		},
	}
	driverFlag.Options["node-volume-size"] = &types.Flag{
		Type:  types.IntPointerType,
		Usage: "The volume size for each node",
		Default: &types.Default{
			DefaultInt: 20,
		},
	}

	driverFlag.Options["virtual-network"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The name of the virtual network to use",
	}
	driverFlag.Options["subnets"] = &types.Flag{
		Type:  types.StringSliceType,
		Usage: "Comma-separated list of subnets in the virtual network to use",
		Default: &types.Default{
			DefaultStringSlice: &types.StringSlice{Value: []string{}}, //avoid nil value for init
		},
	}
	driverFlag.Options["service-role"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The service role to use to perform the cluster operations in AWS",
	}
	driverFlag.Options["security-groups"] = &types.Flag{
		Type:  types.StringSliceType,
		Usage: "Comma-separated list of security groups to use for the cluster",
	}
	driverFlag.Options["ami"] = &types.Flag{
		Type:  types.StringType,
		Usage: "A custom AMI ID to use for the worker nodes instead of the default",
	}
	driverFlag.Options["associate-worker-node-public-ip"] = &types.Flag{
		Type:  types.BoolPointerType,
		Usage: "A custom AMI ID to use for the worker nodes instead of the default",
		Default: &types.Default{
			DefaultBool: true,
		},
	}
	driverFlag.Options["ingress-rules"] = &types.Flag{
		Type:  types.StringSliceType,
		Usage: "Comma-separated list of ingress rules of the form protocol:fromPort[:toPort[:cidrIP]]",
		Default: &types.Default{
			DefaultStringSlice: &types.StringSlice{Value: []string{}}, //avoid nil value for init
		},
	}
	driverFlag.Options["egress-rules"] = &types.Flag{
		Type:  types.StringSliceType,
		Usage: "Comma-separated list of egress rules of the form protocol:fromPort[:toPort[:cidrIP]]",
		Default: &types.Default{
			DefaultStringSlice: &types.StringSlice{Value: []string{}}, //avoid nil value for init
		},
	}
	// Newlines are expected to always be passed as "\n"
	driverFlag.Options["user-data"] = &types.Flag{
		Type:  types.StringType,
		Usage: "Pass user-data to the nodes to perform automated configuration tasks",
		Default: &types.Default{
			DefaultString: "#!/bin/bash\nset -o xtrace\n" +
				"/etc/eks/bootstrap.sh ${ClusterName} ${BootstrapArguments}" +
				"/opt/aws/bin/cfn-signal --exit-code $? " +
				"--stack  ${AWS::StackName} " +
				"--resource NodeGroup --region ${AWS::Region}\n",
		},
	}

	driverFlag.Options["kubernetes-version"] = &types.Flag{
		Type:    types.StringType,
		Usage:   "The kubernetes master version",
		Default: &types.Default{DefaultString: "1.10"},
	}

	driverFlag.Options["tags"] = &types.Flag{
		Type:  types.StringSliceType,
		Usage: "Tags for Kubernetes cluster. For example, foo=bar.",
	}

	driverFlag.Options["key-pair-name"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The ssh key pair to use",
	}

	return &driverFlag, nil
}

func (d *Driver) GetDriverUpdateOptions(ctx context.Context) (*types.DriverFlags, error) {
	driverFlag := types.DriverFlags{
		Options: make(map[string]*types.Flag),
	}

	driverFlag.Options["kubernetes-version"] = &types.Flag{
		Type:    types.StringType,
		Usage:   "The kubernetes version to update",
		Default: &types.Default{DefaultString: "1.10"},
	}
	driverFlag.Options["access-key"] = &types.Flag{
		Type:  types.StringType,
		Usage: "The AWS Client ID to use",
	}
	driverFlag.Options["secret-key"] = &types.Flag{
		Type:     types.StringType,
		Password: true,
		Usage:    "The AWS Client Secret associated with the Client ID",
	}
	driverFlag.Options["session-token"] = &types.Flag{
		Type:  types.StringType,
		Usage: "A session token to use with the client key and secret if applicable.",
	}

	return &driverFlag, nil
}

func getStateFromOptions(driverOptions *types.DriverOptions) (State, error) {
	state := State{}
	state.ClusterName = options.GetValueFromDriverOptions(driverOptions, types.StringType, "name").(string)
	state.DisplayName = options.GetValueFromDriverOptions(driverOptions, types.StringType, "display-name", "displayName").(string)
	state.ClientID = options.GetValueFromDriverOptions(driverOptions, types.StringType, "client-id", "accessKey").(string)
	state.ClientSecret = options.GetValueFromDriverOptions(driverOptions, types.StringType, "client-secret", "secretKey").(string)
	state.SessionToken = options.GetValueFromDriverOptions(driverOptions, types.StringType, "session-token", "sessionToken").(string)
	state.KubernetesVersion = options.GetValueFromDriverOptions(driverOptions, types.StringType, "kubernetes-version", "kubernetesVersion").(string)

	state.Region = options.GetValueFromDriverOptions(driverOptions, types.StringType, "region").(string)
	state.VirtualNetwork = options.GetValueFromDriverOptions(driverOptions, types.StringType, "virtual-network", "virtualNetwork").(string)
	state.Subnets = options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "subnets").(*types.StringSlice).Value
	state.ServiceRole = options.GetValueFromDriverOptions(driverOptions, types.StringType, "service-role", "serviceRole").(string)
	state.SecurityGroups = options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "security-groups", "securityGroups").(*types.StringSlice).Value
	state.AMI = options.GetValueFromDriverOptions(driverOptions, types.StringType, "ami").(string)
	state.AssociateWorkerNodePublicIP, _ = options.GetValueFromDriverOptions(driverOptions, types.BoolPointerType, "associate-worker-node-public-ip", "associateWorkerNodePublicIp").(*bool)

	// UserData
	state.UserData = options.GetValueFromDriverOptions(driverOptions, types.StringType, "user-data", "userData").(string)

	state.IngressRules = options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "ingress-rules").(*types.StringSlice).Value
	state.EgressRules = options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "egress-rules").(*types.StringSlice).Value

	state.Tags = make(map[string]string)
	tagValues := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "tags").(*types.StringSlice).Value
	for _, part := range tagValues {
		kv := strings.Split(part, "=")
		if len(kv) == 2 {
			state.Tags[kv[0]] = kv[1]
		}
	}

	state.KeyPairName = options.GetValueFromDriverOptions(driverOptions, types.StringType, "key-pair-name", "keyPairName").(string)

	InstanceGroupName := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "instance-group-name", "instanceGroupName").(*types.StringSlice).Value
	InstanceType := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "instance-type", "instanceType").(*types.StringSlice).Value
	InstanceCount := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "instance-count", "instanceCount").(*types.StringSlice).Value
	MinimumASGSize := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "minimum-nodes", "minimumNodes").(*types.StringSlice).Value
	MaximumASGSize := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "maximum-nodes", "maximumNodes").(*types.StringSlice).Value
	NodeVolumeSize := options.GetValueFromDriverOptions(driverOptions, types.StringSliceType, "node-volume-size", "nodeVolumeSize").(*types.StringSlice).Value

	fmt.Printf("Inside EKS : InstanceGroupName: %s\n", InstanceGroupName)
	fmt.Printf("Inside EKS : InstanceType: %s\n", InstanceType)
	fmt.Printf("Inside EKS : InstanceCount: %s\n", InstanceCount)
	fmt.Printf("Inside EKS : MinimumASGSize: %s\n", MinimumASGSize)
	fmt.Printf("Inside EKS : MaximumASGSize: %s\n", MaximumASGSize)
	fmt.Printf("Inside EKS : NodeVolumeSize: %s\n", NodeVolumeSize)

	// Assuming number of groups equals to number of group name provided.
	var instanceGroups = make(InstanceGroups, len(InstanceGroupName))
	for i, v := range InstanceGroupName {
		var instanceGroup InstanceGroup
		instanceGroup.InstanceGroupName = v
		instanceGroup.InstanceType = InstanceType[i]
		instanceGroup.InstanceCount, _ = strconv.ParseInt(InstanceCount[i], 10, 64)
		instanceGroup.MinimumASGSize, _ = strconv.ParseInt(MinimumASGSize[i], 10, 64)
		instanceGroup.MaximumASGSize, _ = strconv.ParseInt(MaximumASGSize[i], 10, 64)
		volumeSize, _ := strconv.ParseInt(NodeVolumeSize[i], 10, 64)
		instanceGroup.NodeVolumeSize = &volumeSize
		instanceGroups[i] = instanceGroup
	}
	state.InstanceGroups = instanceGroups

	return state, state.Validate()
}

func convertSecurityGroupRules(ruleStrings []string) ([]securityGroupRule, error) {
	if ruleStrings == nil || len(ruleStrings) == 0 {
		return nil, nil
	}
	rules := make([]securityGroupRule, len(ruleStrings))

	for i, ruleString := range ruleStrings {
		arr := strings.Split(ruleString, ":")
		if len(arr) < 2 {
			return nil, fmt.Errorf("invalid rule %s", ruleString)
		}
		protocol := arr[0]
		if protocol != "tcp" && protocol != "udp" && protocol != "icmp" {
			return nil, fmt.Errorf("invalid rule protocol %s", ruleString)
		}
		fromPort, err := strconv.ParseUint(arr[1], 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid rule fromPort %s", ruleString)
		}
		toPort := fromPort
		if len(arr) > 2 {
			toPort, err = strconv.ParseUint(arr[2], 10, 16)
			if err != nil {
				return nil, fmt.Errorf("invalid rule toPort %s", ruleString)
			}
		}
		cidrIP := "0.0.0.0/0"
		if len(arr) > 3 {
			if _, _, err = net.ParseCIDR(arr[3]); err != nil {
				return nil, fmt.Errorf("invalid rule cidrIP %s", ruleString)
			}
			cidrIP = arr[3]
		}
		rules[i] = securityGroupRule{protocol, uint16(fromPort), uint16(toPort), cidrIP}
	}

	return rules, nil
}

func (state *State) Validate() error {
	if state.DisplayName == "" {
		return fmt.Errorf("display name is required")
	}

	if state.ClientID == "" {
		return fmt.Errorf("client id is required")
	}

	if state.ClientSecret == "" {
		return fmt.Errorf("client secret is required")
	}

	// If no k8s version is set then this is a legacy cluster and we can't choose the correct ami anyway, so skip those
	// validations
	if state.KubernetesVersion != "" {
		amiForRegion, ok := amiForRegionAndVersion[state.KubernetesVersion]
		if !ok && state.AMI == "" {
			return fmt.Errorf("default ami of region %s for kubernetes version %s is not set", state.Region, state.KubernetesVersion)
		}

		// If the custom AMI ID is set, then assume they are trying to spin up in a region we don't have knowledge of
		// and try to create anyway
		if amiForRegion[state.Region] == "" && state.AMI == "" {
			return fmt.Errorf("rancher does not support region %v, no entry for ami lookup", state.Region)
		}
	}

	for _, instanceGroup := range state.InstanceGroups {
		if instanceGroup.MinimumASGSize < 1 {
			return fmt.Errorf("minimum nodes must be greater than 0. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}

		if instanceGroup.MaximumASGSize < 1 {
			return fmt.Errorf("maximum nodes must be greater than 0. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}

		if instanceGroup.MaximumASGSize < instanceGroup.MinimumASGSize {
			return fmt.Errorf("maximum nodes cannot be less than minimum nodes. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}

		if instanceGroup.InstanceCount < instanceGroup.MinimumASGSize {
			return fmt.Errorf("instance count cannot be less than minimum nodes. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}

		if instanceGroup.InstanceCount > instanceGroup.MaximumASGSize {
			return fmt.Errorf("instance count cannot be greater than maximum nodes. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}

		if instanceGroup.NodeVolumeSize != nil && *instanceGroup.NodeVolumeSize < 1 {
			return fmt.Errorf("node volume size must be greater than 0. InstanceGroupName: %s", instanceGroup.InstanceGroupName)
		}
	}

	networkEmpty := state.VirtualNetwork == ""
	subnetEmpty := len(state.Subnets) == 0
	securityGroupEmpty := len(state.SecurityGroups) == 0

	if !(networkEmpty == subnetEmpty && subnetEmpty == securityGroupEmpty) {
		return fmt.Errorf("virtual network, subnet, and security group must all be set together")
	}

	if state.AssociateWorkerNodePublicIP != nil &&
		!*state.AssociateWorkerNodePublicIP &&
		(state.VirtualNetwork == "" || len(state.Subnets) == 0) {
		return fmt.Errorf("if AssociateWorkerNodePublicIP is set to false a VPC and subnets must also be provided")
	}

	return nil
}

func alreadyExistsInCloudFormationError(err error) bool {
	if aerr, ok := err.(awserr.Error); ok {
		switch aerr.Code() {
		case cloudformation.ErrCodeAlreadyExistsException:
			return true
		}
	}

	return false
}

func (d *Driver) createStack(svc *cloudformation.CloudFormation, name string, tags []*cloudformation.Tag,
	templateBody string, capabilities []string, parameters []*cloudformation.Parameter) (*cloudformation.DescribeStacksOutput, error) {
	_, err := svc.CreateStack(&cloudformation.CreateStackInput{
		StackName:    aws.String(name),
		TemplateBody: aws.String(templateBody),
		Capabilities: aws.StringSlice(capabilities),
		Parameters:   parameters,
		Tags:         tags,
	})
	if err != nil && !alreadyExistsInCloudFormationError(err) {
		return nil, fmt.Errorf("error creating master: %v", err)
	}

	var stack *cloudformation.DescribeStacksOutput
	status := "CREATE_IN_PROGRESS"

	for status == "CREATE_IN_PROGRESS" {
		time.Sleep(time.Second * 5)
		stack, err = svc.DescribeStacks(&cloudformation.DescribeStacksInput{
			StackName: aws.String(name),
		})
		if err != nil {
			return nil, fmt.Errorf("error polling stack info: %v", err)
		}

		status = *stack.Stacks[0].StackStatus
	}

	if len(stack.Stacks) == 0 {
		return nil, fmt.Errorf("stack did not have output: %v", err)
	}

	if status != "CREATE_COMPLETE" {
		reason := "reason unknown"
		events, err := svc.DescribeStackEvents(&cloudformation.DescribeStackEventsInput{
			StackName: aws.String(name),
		})
		if err == nil {
			for _, event := range events.StackEvents {
				// guard against nil pointer dereference
				if event.ResourceStatus == nil || event.LogicalResourceId == nil || event.ResourceStatusReason == nil {
					continue
				}

				if *event.ResourceStatus == "CREATE_FAILED" {
					reason = *event.ResourceStatusReason
					break
				}

				if *event.ResourceStatus == "ROLLBACK_IN_PROGRESS" {
					reason = *event.ResourceStatusReason
					// do not break so that CREATE_FAILED takes priority
				}
			}
		}
		return nil, fmt.Errorf("stack failed to create: %v", reason)
	}

	return stack, nil
}

func (d *Driver) changeStack(svc *cloudformation.CloudFormation, changeSetName string, name string, tags []*cloudformation.Tag,
	templateBody string, capabilities []string, parameters []*cloudformation.Parameter) error {

	_, err := svc.CreateChangeSet(&cloudformation.CreateChangeSetInput{
		ChangeSetName: aws.String(changeSetName),
		StackName:     aws.String(name),
		TemplateBody:  aws.String(templateBody),
		Capabilities:  aws.StringSlice(capabilities),
		Parameters:    parameters,
		Tags:          tags,
	})
	if err != nil {
		return fmt.Errorf("error creating change set: %v", err)
	}

	status := "CREATE_IN_PROGRESS"
	var desc *cloudformation.DescribeChangeSetOutput
	for status == "CREATE_IN_PROGRESS" {
		time.Sleep(time.Second * 10)
		desc, err = svc.DescribeChangeSet(&cloudformation.DescribeChangeSetInput{
			ChangeSetName: aws.String(changeSetName),
			StackName:     aws.String(name),
		})
		if err != nil {
			return d.cleanupChangeSet(svc, changeSetName, name, fmt.Errorf("error polling change set status: %v", err))
		}
		status = *desc.Status
	}

	if status != "CREATE_COMPLETE" {
		reason := desc.StatusReason
		return d.cleanupChangeSet(svc, changeSetName, name, fmt.Errorf("change set creation failed: %v", reason))
	}
	logrus.Infof("Change set description:\n%s\n", desc)

	_, err = svc.ExecuteChangeSet(&cloudformation.ExecuteChangeSetInput{
		ChangeSetName: aws.String(changeSetName),
		StackName:     aws.String(name),
	})
	if err != nil {
		return d.cleanupChangeSet(svc, changeSetName, name, fmt.Errorf("error executing change set: %v", err))
	}

	var stack *cloudformation.DescribeStacksOutput
	status = "UPDATE_IN_PROGRESS"
	for status == "UPDATE_IN_PROGRESS" {
		time.Sleep(time.Second * 10)
		stack, err = svc.DescribeStacks(&cloudformation.DescribeStacksInput{
			StackName: aws.String(name),
		})
		if err != nil {
			return fmt.Errorf("error polling stack info: %v", err)
		}

		status = *stack.Stacks[0].StackStatus
	}

	if status != "UPDATE_COMPLETE" {
		return fmt.Errorf("change set execution failed: %s", status)
	}

	return nil
}

func (d *Driver) cleanupChangeSet(svc *cloudformation.CloudFormation, changeSetName string, name string, err error) error {
	_, err2 := svc.DeleteChangeSet(&cloudformation.DeleteChangeSetInput{
		ChangeSetName: aws.String(changeSetName),
		StackName:     aws.String(name),
	})
	if err2 == nil {
		return fmt.Errorf("%v", err)
	} else {
		return fmt.Errorf("%v\nand couldn't delete change set %v", err, err2)
	}
}

func toStringPointerSlice(strings []string) []*string {
	var stringPointers []*string

	for _, stringLiteral := range strings {
		stringPointers = append(stringPointers, aws.String(stringLiteral))
	}

	return stringPointers
}

func toStringLiteralSlice(strings []*string) []string {
	var stringLiterals []string

	for _, stringPointer := range strings {
		stringLiterals = append(stringLiterals, *stringPointer)
	}

	return stringLiterals
}

func (d *Driver) Create(ctx context.Context, options *types.DriverOptions, _ *types.ClusterInfo) (*types.ClusterInfo, error) {
	logrus.Infof("Starting create")

	state, err := getStateFromOptions(options)
	if err != nil {
		return nil, fmt.Errorf("error parsing state: %v", err)
	}

	info := &types.ClusterInfo{}
	storeState(info, state) // store state here and after initialization

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return info, fmt.Errorf("error getting new aws session: %v", err)
	}

	svc := cloudformation.New(sess)

	tags := getTags(state)

	err = d.initVpc(svc, &state, tags)
	if err != nil {
		return info, err
	}

	cluster, err := d.initCluster(svc, sess, &state, tags)
	if err != nil {
		return info, err
	}

	capem, err := base64.StdEncoding.DecodeString(*cluster.Cluster.CertificateAuthority.Data)
	if err != nil {
		return info, fmt.Errorf("error parsing CA data: %v", err)
	}

	ec2svc := ec2.New(sess)
	if state.KeyPairName == "" {
		keyPairName := getEC2KeyPairName(state.DisplayName)
		logrus.Infof("Creating KeyPair:%s", keyPairName)
		_, err = ec2svc.CreateKeyPair(&ec2.CreateKeyPairInput{
			KeyName: aws.String(keyPairName),
		})
		if err != nil && !isDuplicateKeyError(err) {
			return info, fmt.Errorf("error creating key pair %v", err)
		}
		state.KeyPairName = keyPairName
	} else {
		logrus.Infof("KeyPair name is provided, skipping create")
	}

	if state.AMI == "" {
		//should be always accessible after validate()
		state.AMI = getAMIs(ctx, ec2svc, state)
	}

	if state.AssociateWorkerNodePublicIP == nil {
		b := true
		state.AssociateWorkerNodePublicIP = &b
	}

	storeState(info, state) // store state again now that initialization is complete
	var nodeInstanceRoles []string;
	for _, instanceGroup := range state.InstanceGroups {

		workerNodesFinalTemplate, workerParameters, err := getWorkerParameters(state, instanceGroup)
		if err != nil {
			return info, err
		}

		logrus.Infof("Creating worker nodes! Stack-name: %s", getWorkNodeName(instanceGroup.InstanceGroupName))
		stack, err := d.createStack(svc, getWorkNodeName(instanceGroup.InstanceGroupName), tags, workerNodesFinalTemplate,
			[]string{cloudformation.CapabilityCapabilityIam},
			workerParameters)
		if err != nil {
			return info, fmt.Errorf("error creating stack: %v", err)
		}

		nodeInstanceRole := getParameterValueFromOutput("NodeInstanceRole", stack.Stacks[0].Outputs)
		if nodeInstanceRole == "" {
			return info, fmt.Errorf("no node instance role returned in output: %v", err)
		}
		nodeInstanceRoles = append(nodeInstanceRoles, nodeInstanceRole)
	}



	err = d.createConfigMap(state, *cluster.Cluster.Endpoint, capem, nodeInstanceRoles)
	if err != nil {
		return info, err
	}

	return info, nil
}

// Helper struct used inside go func
type NodeInstanceRoleChan struct {
	Arn string
	Err error
}

func getTags(state State) []*cloudformation.Tag {
	tags := []*cloudformation.Tag{}
	tag := &cloudformation.Tag{Key: aws.String("displayName"), Value: aws.String(state.DisplayName)}
	tags = append(tags, tag)
	for key, val := range state.Tags {
		if val != "" {
			tag := &cloudformation.Tag{Key: aws.String(key), Value: aws.String(val)}
			tags = append(tags, tag)
		}
	}
	return tags
}

func (d *Driver) initVpc(svc *cloudformation.CloudFormation, state *State, tags []*cloudformation.Tag) error {
	if state.VirtualNetwork != "" {
		logrus.Infof("VPC info provided, skipping create")
		return nil
	}

	logrus.Infof("Bringing up vpc:%s", getVPCStackName(state.DisplayName))

	stack, err := d.createStack(svc, getVPCStackName(state.DisplayName), tags, vpcTemplate, []string{},
		[]*cloudformation.Parameter{})
	if err != nil {
		return fmt.Errorf("error creating stack: %v", err)
	}

	securityGroupsString := getParameterValueFromOutput("SecurityGroups", stack.Stacks[0].Outputs)
	subnetIdsString := getParameterValueFromOutput("SubnetIds", stack.Stacks[0].Outputs)

	if securityGroupsString == "" || subnetIdsString == "" {
		return fmt.Errorf("no security groups or subnet ids were returned")
	}

	state.SecurityGroups = strings.Split(securityGroupsString, ",")
	state.Subnets = strings.Split(subnetIdsString, ",")

	resources, err := svc.DescribeStackResources(&cloudformation.DescribeStackResourcesInput{
		StackName: aws.String(state.DisplayName + "-eks-vpc"),
	})
	if err != nil {
		return fmt.Errorf("error getting stack resoures")
	}

	for _, resource := range resources.StackResources {
		logrus.Infof("LogicalResourceId: %s PhysicalResourceId: %s", *resource.LogicalResourceId, *resource.PhysicalResourceId)
		if *resource.LogicalResourceId == "VPC" {
			state.VirtualNetwork = *resource.PhysicalResourceId
		}
	}
	return nil
}

func (d *Driver) initRoleARN(svc *cloudformation.CloudFormation, sess *session.Session, state *State, tags []*cloudformation.Tag) (string, error) {
	var roleARN string
	if state.ServiceRole == "" {
		logrus.Infof("Creating service role:%s", getServiceRoleName(state.DisplayName))

		stack, err := d.createStack(svc, getServiceRoleName(state.DisplayName), tags, serviceRoleTemplate,
			[]string{cloudformation.CapabilityCapabilityIam}, nil)
		if err != nil {
			return "", fmt.Errorf("error creating stack: %v", err)
		}

		roleARN = getParameterValueFromOutput("RoleArn", stack.Stacks[0].Outputs)
		if roleARN == "" {
			return "", fmt.Errorf("no RoleARN was returned")
		}
		state.ServiceRole = *stack.Stacks[0].StackName
	} else {
		logrus.Infof("Retrieving existing service role")
		iamClient := iam.New(sess, aws.NewConfig().WithRegion(state.Region))
		role, err := iamClient.GetRole(&iam.GetRoleInput{
			RoleName: aws.String(state.ServiceRole),
		})
		if err != nil {
			return "", fmt.Errorf("error getting role: %v", err)
		}

		roleARN = *role.Role.Arn
	}
	return roleARN, nil
}

func (d *Driver) initCluster(svc *cloudformation.CloudFormation, sess *session.Session, state *State, tags []*cloudformation.Tag) (*eks.DescribeClusterOutput, error) {
	roleARN, err := d.initRoleARN(svc, sess, state, tags)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Creating EKS cluster: %s", state.DisplayName)

	eksService := eks.New(sess)
	_, err = eksService.CreateCluster(&eks.CreateClusterInput{
		Name:    aws.String(state.DisplayName),
		RoleArn: aws.String(roleARN),
		ResourcesVpcConfig: &eks.VpcConfigRequest{
			SecurityGroupIds: toStringPointerSlice(state.SecurityGroups),
			SubnetIds:        toStringPointerSlice(state.Subnets),
		},
		Version: aws.String(state.KubernetesVersion),
	})
	if err != nil && !isClusterConflict(err) {
		return nil, fmt.Errorf("error creating cluster: %v", err)
	}

	cluster, err := d.waitForClusterReady(eksService, *state)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Cluster %s provisioned successfully", state.DisplayName)

	return cluster, nil
}

func getWorkerParameters(state State, instanceGroup InstanceGroup) (string, []*cloudformation.Parameter, error) {
	ingressRules, err := convertSecurityGroupRules(state.IngressRules)
	if err != nil {
		return "", nil, err
	}
	egressRules, err := convertSecurityGroupRules(state.EgressRules)
	if err != nil {
		return "", nil, err
	}

	var rulesString string
	if ingressRules != nil && len(ingressRules) > 0 {
		rulesString = ingressPrefix
		for _, rule := range ingressRules {
			nextRuleString := fmt.Sprintf(securityRuleTemplate, rule.protocol, rule.fromPort, rule.toPort, rule.cidrIP)
			rulesString = fmt.Sprintf("%s%s", rulesString, nextRuleString)
		}
	} else {
		rulesString = ""
	}
	if egressRules != nil && len(egressRules) > 0 {
		rulesString = fmt.Sprintf("%s%s", rulesString, egressPrefix)
		for _, rule := range egressRules {
			nextRuleString := fmt.Sprintf(securityRuleTemplate, rule.protocol, rule.fromPort, rule.toPort, rule.cidrIP)
			rulesString = fmt.Sprintf("%s%s", rulesString, nextRuleString)
		}
	}
	// amend UserData values into template.
	// must use %q to safely pass the string
	workerNodesFinalTemplate := fmt.Sprintf(workerNodesTemplate, rulesString, state.UserData)
	logrus.Debug(workerNodesFinalTemplate)

	return workerNodesFinalTemplate, []*cloudformation.Parameter{
		{ParameterKey: aws.String("ClusterName"), ParameterValue: aws.String(state.DisplayName)},
		{ParameterKey: aws.String("ClusterControlPlaneSecurityGroup"),
			ParameterValue: aws.String(strings.Join(state.SecurityGroups, ","))},
		{ParameterKey: aws.String("NodeGroupName"),
			ParameterValue: aws.String(getNodeGroupName(instanceGroup.InstanceGroupName))},
		{ParameterKey: aws.String("NodeAutoScalingGroupDesiredCapacity"), ParameterValue: aws.String(strconv.Itoa(
			int(instanceGroup.InstanceCount)))},
		{ParameterKey: aws.String("NodeAutoScalingGroupMinSize"), ParameterValue: aws.String(strconv.Itoa(
			int(instanceGroup.MinimumASGSize)))},
		{ParameterKey: aws.String("NodeAutoScalingGroupMaxSize"), ParameterValue: aws.String(strconv.Itoa(
			int(instanceGroup.MaximumASGSize)))},
		{ParameterKey: aws.String("NodeVolumeSize"), ParameterValue: aws.String(strconv.Itoa(
			int(*instanceGroup.NodeVolumeSize)))},
		{ParameterKey: aws.String("NodeInstanceType"), ParameterValue: aws.String(instanceGroup.InstanceType)},
		{ParameterKey: aws.String("NodeImageId"), ParameterValue: aws.String(state.AMI)},
		{ParameterKey: aws.String("KeyName"), ParameterValue: aws.String(state.KeyPairName)},
		{ParameterKey: aws.String("VpcId"), ParameterValue: aws.String(state.VirtualNetwork)},
		{ParameterKey: aws.String("Subnets"),
			ParameterValue: aws.String(strings.Join(state.Subnets, ","))},
		{ParameterKey: aws.String("PublicIp"), ParameterValue: aws.String(strconv.FormatBool(*state.AssociateWorkerNodePublicIP))},
	}, nil
}

func (d *Driver) changeWorkers(ctx context.Context, info *types.ClusterInfo, state State) (*types.ClusterInfo, error) {
	logrus.Infof("New State Info %+v", state)

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return info, fmt.Errorf("error getting new aws session: %v", err)
	}

	svc := cloudformation.New(sess)

	tags := getTags(state)

	if state.VirtualNetwork == "" {
		return info, fmt.Errorf("virtual network should already be set")
	}

	if state.AMI == "" {
		return info, fmt.Errorf("AMI should already be set")
	}

	if state.AssociateWorkerNodePublicIP == nil {
		b := true
		state.AssociateWorkerNodePublicIP = &b
	}

	for _, instanceGroup := range state.InstanceGroups {

		workerNodesFinalTemplate, workerParameters, err := getWorkerParameters(state, instanceGroup)
		if err != nil {
			return info, err
		}

		logrus.Infof("Changing worker nodes! Stack-name: %s", getWorkNodeName(instanceGroup.InstanceGroupName))
		err = d.changeStack(svc, getChangeSetName(state.DisplayName), getWorkNodeName(instanceGroup.InstanceGroupName), tags, workerNodesFinalTemplate,
			[]string{cloudformation.CapabilityCapabilityIam},
			workerParameters)
		if err != nil {
			return info, fmt.Errorf("error changing stack: %v", err)
		}
	}

	return info, nil
}

func isDuplicateKeyError(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

func isClusterConflict(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		return awsErr.Code() == eks.ErrCodeResourceInUseException
	}

	return false
}

func getEC2KeyPairName(name string) string {
	return name + "-ec2-key-pair"
}

func getServiceRoleName(name string) string {
	return name + "-eks-service-role"
}

func getVPCStackName(name string) string {
	return name + "-eks-vpc"
}

func getWorkNodeName(name string) string {
	return name + "-eks-worker-nodes"
}

func getNodeGroupName(name string) string {
	return name + "-node-group"
}

func getChangeSetName(name string) string {
	return name + "-eks-worker-nodes-change-set"
}

func (d *Driver) createConfigMap(state State, endpoint string, capem []byte, nodeInstanceRoles []string) error {
	clientset, err := createClientset(state.DisplayName, state, endpoint, capem)
	if err != nil {
		return fmt.Errorf("error creating clientset: %v", err)
	}
	data := make([]map[string]interface{}, 0)
	for _, nodeInstanceRole := range nodeInstanceRoles {
		arn := map[string]interface{}{
			"rolearn":  nodeInstanceRole,
			"username": "system:node:{{EC2PrivateDNSName}}",
			"groups": []string{
				"system:bootstrappers",
				"system:nodes",
			},
		}
		data = append(data, arn)
	}
	fmt.Printf("Data %+v\n", data)
	mapRoles, err := yaml.Marshal(data)
	if err != nil {
		return fmt.Errorf("error marshalling map roles: %v", err)
	}

	logrus.Infof("Applying ConfigMap")

	_, err = clientset.CoreV1().ConfigMaps("kube-system").Create(&v1.ConfigMap{
		TypeMeta: v12.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: v12.ObjectMeta{
			Name:      "aws-auth",
			Namespace: "kube-system",
		},
		Data: map[string]string{
			"mapRoles": string(mapRoles),
		},
	})
	if err != nil && !errors.IsConflict(err) {
		return fmt.Errorf("error creating config map: %v", err)
	}

	return nil
}

func createClientset(name string, state State, endpoint string, capem []byte) (*kubernetes.Clientset, error) {
	token, err := getEKSToken(name, state)
	if err != nil {
		return nil, fmt.Errorf("error generating token: %v", err)
	}

	config := &rest.Config{
		Host: endpoint,
		TLSClientConfig: rest.TLSClientConfig{
			CAData: capem,
		},
		BearerToken: token,
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("error creating clientset: %v", err)
	}

	return clientset, nil
}

const awsCredentialsDirectory = "./management-state/aws"
const awsCredentialsPath = awsCredentialsDirectory + "/credentials"
const awsSharedCredentialsFile = "AWS_SHARED_CREDENTIALS_FILE"

var awsCredentialsLocker = &sync.Mutex{}

func getEKSToken(name string, state State) (string, error) {
	generator, err := heptio.NewGenerator()
	if err != nil {
		return "", fmt.Errorf("error creating generator: %v", err)
	}

	defer awsCredentialsLocker.Unlock()
	awsCredentialsLocker.Lock()
	os.Setenv(awsSharedCredentialsFile, awsCredentialsPath)

	defer func() {
		os.Remove(awsCredentialsPath)
		os.Remove(awsCredentialsDirectory)
		os.Unsetenv(awsSharedCredentialsFile)
	}()
	err = os.MkdirAll(awsCredentialsDirectory, 0744)
	if err != nil {
		return "", fmt.Errorf("error creating credentials directory: %v", err)
	}

	var credentialsContent string
	if state.SessionToken == "" {
		credentialsContent = fmt.Sprintf(
			`[default]
aws_access_key_id=%v
aws_secret_access_key=%v`,
			state.ClientID,
			state.ClientSecret)
	} else {
		credentialsContent = fmt.Sprintf(
			`[default]
aws_access_key_id=%v
aws_secret_access_key=%v
aws_session_token=%v`,
			state.ClientID,
			state.ClientSecret,
			state.SessionToken)
	}

	err = ioutil.WriteFile(awsCredentialsPath, []byte(credentialsContent), 0644)
	if err != nil {
		return "", fmt.Errorf("error writing credentials file: %v", err)
	}

	return generator.Get(name)
}

func (d *Driver) waitForClusterReady(svc *eks.EKS, state State) (*eks.DescribeClusterOutput, error) {
	var cluster *eks.DescribeClusterOutput
	var err error

	status := ""
	for status != "ACTIVE" {
		time.Sleep(30 * time.Second)

		logrus.Infof("Waiting for cluster to finish provisioning")

		cluster, err = svc.DescribeCluster(&eks.DescribeClusterInput{
			Name: aws.String(state.DisplayName),
		})
		if err != nil {
			return nil, fmt.Errorf("error polling cluster state: %v", err)
		}

		if cluster.Cluster == nil {
			return nil, fmt.Errorf("no cluster data was returned")
		}

		if cluster.Cluster.Status == nil {
			return nil, fmt.Errorf("no cluster status was returned")
		}

		status = *cluster.Cluster.Status
	}

	return cluster, nil
}

func storeState(info *types.ClusterInfo, state State) error {
	data, err := json.Marshal(state)

	if err != nil {
		return err
	}

	if info.Metadata == nil {
		info.Metadata = map[string]string{}
	}

	info.Metadata["state"] = string(data)

	return nil
}

func getState(info *types.ClusterInfo) (State, error) {
	state := State{}

	err := json.Unmarshal([]byte(info.Metadata["state"]), &state)
	if err != nil {
		logrus.Errorf("Error encountered while marshalling state: %v", err)
	}

	return state, err
}

func getParameterValueFromOutput(key string, outputs []*cloudformation.Output) string {
	for _, output := range outputs {
		if *output.OutputKey == key {
			return *output.OutputValue
		}
	}

	return ""
}

func (d *Driver) Update(ctx context.Context, info *types.ClusterInfo, options *types.DriverOptions) (*types.ClusterInfo, error) {
	logrus.Infof("Starting update")
	oldstate := &State{}
	state, err := getState(info)
	if err != nil {
		return nil, err
	}
	*oldstate = state

	newState, err := getStateFromOptions(options)
	if err != nil {
		return nil, err
	}

	if newState.KubernetesVersion != "" && newState.KubernetesVersion != state.KubernetesVersion {
		state.KubernetesVersion = newState.KubernetesVersion

		if err := d.updateClusterAndWait(ctx, state); err != nil {
			logrus.Errorf("error updating cluster: %v", err)
			return info, err
		}
	}

	for i, instanceGroup := range newState.InstanceGroups {
		// TODO support changing other properties?
		// TODO different getStateFromOptions validation for update vs. create?
		if instanceGroup.InstanceCount != 0 {
			state.InstanceGroups[i].InstanceCount = instanceGroup.InstanceCount
		}
		if instanceGroup.MinimumASGSize != 0 {
			state.InstanceGroups[i].MinimumASGSize = instanceGroup.MinimumASGSize
		}
		if instanceGroup.MaximumASGSize != 0 {
			state.InstanceGroups[i].MaximumASGSize = instanceGroup.MaximumASGSize
		}
	}

	if len(newState.IngressRules) > 0 {
		state.IngressRules = newState.IngressRules
	}
	if len(newState.EgressRules) > 0 {
		state.EgressRules = newState.EgressRules
	}
	if len(newState.Tags) > 0 {
		state.Tags = newState.Tags
	}

	if !reflect.DeepEqual(state, *oldstate) {
		_, err = d.changeWorkers(ctx, info, state)
		if err != nil {
			logrus.Infof("cluster not changed: %v", err)
			return nil, err
		}
	}
	logrus.Infof("Update complete")
	return info, storeState(info, state)
}

func (d *Driver) PostCheck(ctx context.Context, info *types.ClusterInfo) (*types.ClusterInfo, error) {
	logrus.Infof("Starting post-check")

	clientset, err := getClientset(info)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Generating service account token")

	info.ServiceAccountToken, err = util.GenerateServiceAccountToken(clientset)
	if err != nil {
		return nil, fmt.Errorf("error generating service account token: %v", err)
	}

	return info, nil
}

func getClientset(info *types.ClusterInfo) (*kubernetes.Clientset, error) {
	state, err := getState(info)
	if err != nil {
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return nil, fmt.Errorf("error creating new session: %v", err)
	}

	svc := eks.New(sess)
	cluster, err := svc.DescribeCluster(&eks.DescribeClusterInput{
		Name: aws.String(state.DisplayName),
	})
	if err != nil {
		if notFound(err) {
			cluster, err = svc.DescribeCluster(&eks.DescribeClusterInput{
				Name: aws.String(state.ClusterName),
			})
		}

		if err != nil {
			return nil, fmt.Errorf("error getting cluster: %v", err)
		}
	}

	capem, err := base64.StdEncoding.DecodeString(*cluster.Cluster.CertificateAuthority.Data)
	if err != nil {
		return nil, fmt.Errorf("error parsing CA data: %v", err)
	}

	info.Endpoint = *cluster.Cluster.Endpoint
	info.Version = *cluster.Cluster.Version
	info.RootCaCertificate = *cluster.Cluster.CertificateAuthority.Data

	clientset, err := createClientset(state.DisplayName, state, *cluster.Cluster.Endpoint, capem)
	if err != nil {
		return nil, fmt.Errorf("error creating clientset: %v", err)
	}

	_, err = clientset.ServerVersion()
	if err != nil {
		if errors.IsUnauthorized(err) {
			clientset, err = createClientset(state.ClusterName, state, *cluster.Cluster.Endpoint, capem)
			if err != nil {
				return nil, err
			}

			_, err = clientset.ServerVersion()
		}

		if err != nil {
			return nil, err
		}
	}

	return clientset, nil
}

func (d *Driver) Remove(ctx context.Context, info *types.ClusterInfo) error {
	logrus.Infof("Starting delete cluster")

	state, err := getState(info)
	if err != nil {
		return fmt.Errorf("error getting state: %v", err)
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return fmt.Errorf("error getting new aws session: %v", err)
	}

	eksSvc := eks.New(sess)
	_, err = eksSvc.DeleteCluster(&eks.DeleteClusterInput{
		Name: aws.String(state.DisplayName),
	})
	if err != nil {
		if notFound(err) {
			_, err = eksSvc.DeleteCluster(&eks.DeleteClusterInput{
				Name: aws.String(state.ClusterName),
			})
		}

		if err != nil && !notFound(err) {
			return fmt.Errorf("error deleting cluster: %v", err)
		}
	}

	svc := cloudformation.New(sess)

	err = deleteStack(svc, getServiceRoleName(state.DisplayName))
	if err != nil {
		return fmt.Errorf("error deleting service role stack: %v", err)
	}

	err = deleteStack(svc, getVPCStackName(state.DisplayName))
	if err != nil {
		return fmt.Errorf("error deleting vpc stack: %v", err)
	}

	for _, instanceGroup := range state.InstanceGroups {
		err = deleteStack(svc, getWorkNodeName(instanceGroup.InstanceGroupName))
		if err != nil {
			return fmt.Errorf("error deleting worker node stack: %v", err)
		}
	}

	ec2svc := ec2.New(sess)

	name := state.DisplayName
	_, err = ec2svc.DescribeKeyPairs(&ec2.DescribeKeyPairsInput{
		KeyNames: []*string{aws.String(getEC2KeyPairName(name))},
	})
	if doesNotExist(err) {
		name = state.ClusterName
	}

	_, err = ec2svc.DeleteKeyPair(&ec2.DeleteKeyPairInput{
		KeyName: aws.String(getEC2KeyPairName(name)),
	})
	if err != nil {
		return fmt.Errorf("error deleting key pair: %v", err)
	}

	return err
}

func deleteStack(svc *cloudformation.CloudFormation, name string) error {

	_, err := svc.DeleteStack(&cloudformation.DeleteStackInput{
		StackName: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("error deleting stack: %v", err)
	}

	return nil
}

func notFound(err error) bool {
	if awsErr, ok := err.(awserr.Error); ok {
		return awsErr.Code() == eks.ErrCodeResourceNotFoundException
	}

	return false
}

func doesNotExist(err error) bool {
	// There is no better way of doing this because AWS API does not distinguish between a attempt to delete a stack
	// (or key pair) that does not exist, and, for example, a malformed delete request, so we have to parse the error
	// message
	if err != nil {
		return strings.Contains(err.Error(), "does not exist")
	}

	return false
}

func (d *Driver) GetCapabilities(ctx context.Context) (*types.Capabilities, error) {
	return &d.driverCapabilities, nil
}

func (d *Driver) ETCDSave(ctx context.Context, clusterInfo *types.ClusterInfo, opts *types.DriverOptions, snapshotName string) error {
	return fmt.Errorf("ETCD backup operations are not implemented")
}

func (d *Driver) ETCDRestore(ctx context.Context, clusterInfo *types.ClusterInfo, opts *types.DriverOptions, snapshotName string) (*types.ClusterInfo, error) {
	return nil, fmt.Errorf("ETCD backup operations are not implemented")
}

func (d *Driver) GetK8SCapabilities(ctx context.Context, _ *types.DriverOptions) (*types.K8SCapabilities, error) {
	return &types.K8SCapabilities{
		L4LoadBalancer: &types.LoadBalancerCapabilities{
			Enabled:              true,
			Provider:             "ELB",
			ProtocolsSupported:   []string{"TCP"},
			HealthCheckSupported: true,
		},
	}, nil
}

func (d *Driver) GetVersion(ctx context.Context, info *types.ClusterInfo) (*types.KubernetesVersion, error) {
	cluster, err := d.getClusterStats(ctx, info)
	if err != nil {
		return nil, err
	}

	version := &types.KubernetesVersion{Version: *cluster.Version}

	return version, nil
}

func (d *Driver) getClusterStats(ctx context.Context, info *types.ClusterInfo) (*eks.Cluster, error) {
	state, err := getState(info)
	if err != nil {
		return nil, err
	}
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return nil, err
	}

	svc := eks.New(sess)
	cluster, err := svc.DescribeClusterWithContext(ctx, &eks.DescribeClusterInput{
		Name: aws.String(state.DisplayName),
	})
	if err != nil {
		return nil, err
	}

	return cluster.Cluster, nil
}

func (d *Driver) SetVersion(ctx context.Context, info *types.ClusterInfo, version *types.KubernetesVersion) error {
	logrus.Info("updating kubernetes version")
	state, err := getState(info)
	if err != nil {
		return err
	}

	state.KubernetesVersion = version.Version
	if err := d.updateClusterAndWait(ctx, state); err != nil {
		return err
	}

	logrus.Info("kubernetes version update success")
	return nil
}

func (d *Driver) updateClusterAndWait(ctx context.Context, state State) error {
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(state.Region),
		Credentials: credentials.NewStaticCredentials(
			state.ClientID,
			state.ClientSecret,
			state.SessionToken,
		),
	})
	if err != nil {
		return err
	}

	svc := eks.New(sess)
	input := &eks.UpdateClusterVersionInput{
		Name: aws.String(state.DisplayName),
	}
	if state.KubernetesVersion != "" {
		input.Version = aws.String(state.KubernetesVersion)
	}

	output, err := svc.UpdateClusterVersionWithContext(ctx, input)
	if err != nil {
		if notFound(err) {
			input.Name = aws.String(state.ClusterName)
			output, err = svc.UpdateClusterVersionWithContext(ctx, input)
		}

		if err != nil {
			return err
		}
	}

	return d.waitForClusterUpdateReady(ctx, svc, state, *output.Update.Id)
}

func (d *Driver) waitForClusterUpdateReady(ctx context.Context, svc *eks.EKS, state State, updateID string) error {
	logrus.Infof("waiting for update id[%s] state", updateID)
	var update *eks.DescribeUpdateOutput
	var err error

	status := ""
	for status != "Successful" {
		time.Sleep(30 * time.Second)

		logrus.Infof("Waiting for cluster update to finish updating")

		update, err = svc.DescribeUpdateWithContext(ctx, &eks.DescribeUpdateInput{
			Name:     aws.String(state.DisplayName),
			UpdateId: aws.String(updateID),
		})
		if err != nil {
			if notFound(err) {
				update, err = svc.DescribeUpdateWithContext(ctx, &eks.DescribeUpdateInput{
					Name:     aws.String(state.ClusterName),
					UpdateId: aws.String(updateID),
				})
			}

			if err != nil {
				return fmt.Errorf("error polling cluster update state: %v", err)
			}
		}

		if update.Update == nil {
			return fmt.Errorf("no cluster update data was returned")
		}

		if update.Update.Status == nil {
			return fmt.Errorf("no cluster update status aws returned")
		}

		status = *update.Update.Status
	}

	return nil
}

func getAMIs(ctx context.Context, ec2svc *ec2.EC2, state State) string {
	if rtn := getLocalAMI(state); rtn != "" {
		return rtn
	}
	version := state.KubernetesVersion
	output, err := ec2svc.DescribeImagesWithContext(ctx, &ec2.DescribeImagesInput{
		Filters: []*ec2.Filter{
			&ec2.Filter{
				Name:   aws.String("is-public"),
				Values: aws.StringSlice([]string{"true"}),
			},
			&ec2.Filter{
				Name:   aws.String("state"),
				Values: aws.StringSlice([]string{"available"}),
			},
			&ec2.Filter{
				Name:   aws.String("image-type"),
				Values: aws.StringSlice([]string{"machine"}),
			},
			&ec2.Filter{
				Name:   aws.String("name"),
				Values: aws.StringSlice([]string{fmt.Sprintf("%s%s*", amiNamePrefix, version)}),
			},
		},
	})
	if err != nil {
		logrus.WithError(err).Warn("getting AMIs from aws error")
		return ""
	}
	prefix := fmt.Sprintf("%s%s", amiNamePrefix, version)
	rtnImage := ""
	for _, image := range output.Images {
		if *image.State != "available" ||
			*image.ImageType != "machine" ||
			!*image.Public ||
			image.Name == nil ||
			!strings.HasPrefix(*image.Name, prefix) {
			continue
		}
		if *image.ImageId > rtnImage {
			rtnImage = *image.ImageId
		}
	}
	if rtnImage == "" {
		logrus.Warnf("no AMI id was returned")
		return ""
	}
	return rtnImage
}

func getLocalAMI(state State) string {
	amiForRegion, ok := amiForRegionAndVersion[state.KubernetesVersion]
	if !ok {
		return ""
	}
	return amiForRegion[state.Region]
}

func (d *Driver) RemoveLegacyServiceAccount(ctx context.Context, info *types.ClusterInfo) error {
	clientset, err := getClientset(info)
	if err != nil {
		return nil
	}

	return util.DeleteLegacyServiceAccountAndRoleBinding(clientset)
}
