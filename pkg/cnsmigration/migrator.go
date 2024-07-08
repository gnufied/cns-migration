package cnsmigration

import (
	"context"
	"fmt"
	"time"

	"github.com/gnufied/cns-migration/pkg/cnsmigration/vclib"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
	operatorclient "github.com/openshift/client-go/operator/clientset/versioned"
	"github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	vim "github.com/vmware/govmomi/vim25/types"
	"gopkg.in/gcfg.v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
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

var (
	resyncPeriod = 10 * time.Minute
)

type CNSVolumeMigrator struct {
	clientSet                  kubernetes.Interface
	destinationDatastore       string
	sourceDatastore            string
	vSphereConnection          *vclib.VSphereConnection
	kubeConfig                 *rest.Config
	openshiftConfigClientSet   configclient.Interface
	openshiftOperatorClientSet operatorclient.Interface

	// for storing result and matches
	matchingCnsVolumes []types.CnsVolume
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

	err = c.getCNSVolumes(ctx, c.sourceDatastore)
	if err != nil {
		klog.Errorf("error listing cns volumes: %v", err)
		return err
	}
	informerFactory := informers.NewSharedInformerFactory(c.clientSet, resyncPeriod)

	pvList, err := c.clientSet.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	usedVolumeCache := NewInUseStore(pvList.Items)

	// TODO: setup event handler for pods and make sure we are doing the right thing

	c.findCSIVolumes(ctx)

	// find all persistnet volumes which are not attached to any node.
	// group the persistent volumes by nodes
	return nil
}

func (c *CNSVolumeMigrator) setupPodsUsingVolumes(ctx context.Context, informerFactory informers.SharedInformerFactory) error {
	podInformer := informerFactory.Core().V1().Pods()
	podInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    nil,
		UpdateFunc: nil,
		DeleteFunc: nil,
	}, resyncPeriod)
	return nil
}

func (c *CNSVolumeMigrator) findCSIVolumes(ctx context.Context) ([]*v1.PersistentVolume, error) {
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

func (c *CNSVolumeMigrator) getCNSVolumes(ctx context.Context, dsName string) error {
	ds, err := c.getDatastore(ctx, dsName)
	if err != nil {
		return err
	}

	cnsQueryFilter := types.CnsQueryFilter{
		Datastores: []vim.ManagedObjectReference{ds.Reference()},
	}
	for {
		res, err := c.vSphereConnection.CnsClient().QueryVolume(ctx, cnsQueryFilter)
		if err != nil {
			return err
		}
		c.matchingCnsVolumes = append(c.matchingCnsVolumes, res.Volumes...)
		if res.Cursor.Offset == res.Cursor.TotalRecords || len(res.Volumes) == 0 {
			break
		}
		cnsQueryFilter.Cursor = &res.Cursor
	}
	return nil
}

func (c *CNSVolumeMigrator) getDatastore(ctx context.Context, dsName string) (*object.Datastore, error) {
	finder := find.NewFinder(c.vSphereConnection.VimClient(), false)
	dc, err := finder.Datacenter(ctx, c.vSphereConnection.DefaultDatacenter())
	if err != nil {
		return nil, fmt.Errorf("can't find datacenters %s", c.vSphereConnection.DefaultDatacenter())
	}

	finder.SetDatacenter(dc)
	ds, err := finder.Datastore(ctx, dsName)
	if err != nil {
		return nil, fmt.Errorf("error finding datastore %s in datacenter %s", dsName, c.vSphereConnection.DefaultDatacenter())
	}
	return ds, nil
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
