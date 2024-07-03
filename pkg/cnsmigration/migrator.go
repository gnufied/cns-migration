package cnsmigration

import (
	"context"
	"fmt"

	"github.com/gnufied/cns-migration/pkg/cnsmigration/vclib"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
	operatorclient "github.com/openshift/client-go/operator/clientset/versioned"
	"gopkg.in/gcfg.v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/legacy-cloud-providers/vsphere"
)

func SayHello() {
	fmt.Println("Hello World")
}

const (
	vSphereCSIDriverName = "csi.vsphere.vmware.com"
	cloudConfigNamespace = "openshift-config"
	clientOperatorName   = "cns-migrator"
	cloudCredSecretName  = "vmware-vsphere-cloud-credentials"
	secretNamespace      = "openshift-cluster-csi-drivers"
)

type CNSVolumeMigrator struct {
	clientSet                  kubernetes.Interface
	destinationDatastore       string
	sourceDatastore            string
	vSphereConnection          *vclib.VSphereConnection
	kubeConfig                 *rest.Config
	openshiftConfigClientSet   configclient.Interface
	openshiftOperatorClientSet operatorclient.Interface
}

func NewCNSVolumeMigrator(config *rest.Config, dsSource, dsTarget string) *CNSVolumeMigrator {
	return &CNSVolumeMigrator{
		kubeConfig:           config,
		sourceDatastore:      dsSource,
		destinationDatastore: dsTarget,
	}
}

func (c *CNSVolumeMigrator) createOpenshiftClients() {
	// create the clientset
	c.clientSet = kubernetes.NewForConfigOrDie(c.kubeConfig)

	c.openshiftConfigClientSet = configclient.NewForConfigOrDie(rest.AddUserAgent(c.kubeConfig, clientOperatorName))
	c.openshiftOperatorClientSet = operatorclient.NewForConfigOrDie(rest.AddUserAgent(c.kubeConfig, clientOperatorName))
}

func (c *CNSVolumeMigrator) StartMigration(ctx context.Context) error {
	// find existing CSI based persistent volumes, which same datastore as one mentioned in the source
	c.createOpenshiftClients()

	err := c.loginToVCenter(ctx)
	if err != nil {
		klog.Errorf("error logging into vcenter: %v", err)
		return err
	}

	c.findCSIVolumes(ctx)

	// find all persistnet volumes which are not attached to any node.
	// group the persistent volumes by nodes
	return nil
}

func (c *CNSVolumeMigrator) findCSIVolumes(ctx context.Context) ([]*v1.PersistentVolume, error) {
	pvList, err := c.clientSet.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pv := range pvList.Items {
		fmt.Printf("name of pv is %s\n", pv.Name)
		csiSource := pv.Spec.CSI
		if csiSource != nil && (csiSource.Driver == vSphereCSIDriverName && c.checkForDatastore(*csiSource)) {

		}
	}
	return nil, nil
}

func (c *CNSVolumeMigrator) checkForDatastore(csiSource v1.CSIPersistentVolumeSource) bool {
	return true
}

func (c *CNSVolumeMigrator) getCNSVolumes() error {

}

func (c *CNSVolumeMigrator) loginToVCenter(ctx context.Context) error {
	infra, err := c.openshiftConfigClientSet.ConfigV1().Infrastructures().Get(ctx, "cluster", metav1.GetOptions{})
	if err != nil {
		klog.Errorf("error getting infrastrucure object: %v", infra)
		return err
	}

	klog.V(3).Infof("Creating vSphere connection")
	cloudConfig := infra.Spec.CloudConfig
	cloudConfigMap, err := c.clientSet.CoreV1().ConfigMaps(cloudConfigNamespace).Get(ctx, cloudConfig.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get cloud config: %v", err)
	}

	cfgString, ok := cloudConfigMap.Data[infra.Spec.CloudConfig.Key]
	if !ok {
		return fmt.Errorf("cloud config %s/%s does not contain key %q", cloudConfigNamespace, cloudConfig.Name, cloudConfig.Key)
	}
	cfg := new(vsphere.VSphereConfig)
	err = gcfg.ReadStringInto(cfg, cfgString)
	if err != nil {
		return err
	}

	secret, err := c.clientSet.CoreV1().Secrets(secretNamespace).Get(ctx, cloudCredSecretName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	userKey := cfg.Workspace.VCenterIP + "." + "username"
	username, ok := secret.Data[userKey]
	if !ok {
		return fmt.Errorf("error parsing secret %q: key %q not found", cloudCredSecretName, userKey)
	}
	passwordKey := cfg.Workspace.VCenterIP + "." + "password"
	password, ok := secret.Data[passwordKey]
	if !ok {
		return fmt.Errorf("error parsing secret %q: key %q not found", cloudCredSecretName, passwordKey)
	}
	vs := vclib.NewVSphereConnection(string(username), string(password), cfg)
	c.vSphereConnection = vs
	return nil
}
