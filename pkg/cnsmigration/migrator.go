package cnsmigration

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/gnufied/cns-migration/pkg/cnsmigration/vclib"
	configclient "github.com/openshift/client-go/config/clientset/versioned"
	operatorclient "github.com/openshift/client-go/operator/clientset/versioned"
	"github.com/vmware/govmomi/cns/types"
	"github.com/vmware/govmomi/find"
	"github.com/vmware/govmomi/object"
	"github.com/vmware/govmomi/vim25/soap"
	vim "github.com/vmware/govmomi/vim25/types"
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
	usedVolumeCache    *inUseVolumeStore
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

func (c *CNSVolumeMigrator) StartMigration(ctx context.Context, volumeFile string) error {
	// find existing CSI based persistent volumes, which same datastore as one mentioned in the source
	c.createOpenshiftClients()

	err := c.loginToVCenter(ctx)
	if err != nil {
		klog.Errorf("error logging into vcenter: %v", err)
		return err
	}

	klog.Infof("logging successfully to vcenter")

	err = c.getCNSVolumes(ctx, c.sourceDatastore)
	if err != nil {
		klog.Errorf("error listing cns volumes: %v", err)
		return err
	}
	pvList, err := c.clientSet.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	podList, err := c.clientSet.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}

	c.usedVolumeCache = NewInUseStore(pvList.Items)
	c.usedVolumeCache.addAllPods(podList.Items)
	return c.findCSIVolumes(ctx, volumeFile)
}

func (c *CNSVolumeMigrator) findCSIVolumes(ctx context.Context, volumeFile string) error {
	file, err := os.Open(volumeFile)
	if err != nil {
		msg := fmt.Errorf("error opening file %s: %v", volumeFile, err)
		klog.Error(msg)
		return msg
	}
	defer file.Close()

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		msg := fmt.Errorf("error reading file %s: %v", volumeFile, err)
		klog.Error(msg)
		return msg
	}
	if len(fileBytes) == 0 {
		msg := fmt.Errorf("file %s has no listed volumes", volumeFile)
		return msg
	}

	volumeLines := strings.Split(string(fileBytes), "\n")
	for _, volumeLine := range volumeLines {
		pvName := strings.TrimSpace(volumeLine)
		if pvName == "" {
			continue
		}
		klog.Infof("Starting migration for pv %s", pvName)
		pv, err := c.clientSet.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
		if err != nil {
			msg := fmt.Errorf("error finding pv %s: %v", pvName, err)
			klog.Error(msg)
			return msg
		}

		csiSource := pv.Spec.CSI
		if csiSource != nil && csiSource.Driver == vSphereCSIDriverName {
			if c.checkForDatastore(pv) {
				err = c.migrateVolume(ctx, csiSource.VolumeHandle)
				if err != nil {
					klog.Errorf("error migrating volume %s with error: %v", pvName, err)
				} else {
					klog.Infof("successfully migrated pv %s with volumeID: %s", pvName, csiSource.VolumeHandle)
				}
			}
		} else {
			klog.Infof("pv %s is not of expected type", pvName)
		}
	}
	return nil
}

func (c *CNSVolumeMigrator) migrateVolume(ctx context.Context, volumeID string) error {
	dtsDatastore, err := c.getDatastore(ctx, c.destinationDatastore)
	if err != nil {
		msg := fmt.Errorf("error finding destinationd datastore %s: %v", dtsDatastore, err)
		klog.Error(msg)
		return msg
	}

	relocatedSpec := types.NewCnsBlockVolumeRelocateSpec(volumeID, dtsDatastore.Reference())
	task, err := c.vSphereConnection.CnsClient().RelocateVolume(ctx, relocatedSpec)
	if err != nil {
		// Handle case when target DS is same as source DS, i.e. volume has
		// already relocated.
		if soap.IsSoapFault(err) {
			soapFault := soap.ToSoapFault(err)
			klog.Errorf("type of fault: %v. SoapFault Info: %v", reflect.TypeOf(soapFault.VimFault()), soapFault)
			_, isAlreadyExistErr := soapFault.VimFault().(vim.AlreadyExists)
			if isAlreadyExistErr {
				// Volume already exists in the target SP, hence return success.
				return nil
			}
		}
		return err
	}
	taskInfo, err := task.WaitForResultEx(ctx)
	if err != nil {
		msg := fmt.Errorf("error waiting for relocation task: %v", err)
		klog.Error(msg)
		return msg
	}
	results := taskInfo.Result.(types.CnsVolumeOperationBatchResult)
	for _, result := range results.VolumeResults {
		fault := result.GetCnsVolumeOperationResult().Fault
		if fault != nil {
			klog.Errorf("Fault: %+v encountered while relocating volume %v", fault, volumeID)
			return fmt.Errorf(fault.LocalizedMessage)
		}
	}
	return nil
}

func (c *CNSVolumeMigrator) checkForDatastore(pv *v1.PersistentVolume) bool {
	csiSource := pv.Spec.CSI
	for _, cnsVolume := range c.matchingCnsVolumes {
		vh := csiSource.VolumeHandle
		if cnsVolume.VolumeId.Id == vh {
			klog.Infof("found a volume to migrate: %s", vh)
			pvcName, podName, inUseFlag := c.usedVolumeCache.volumeInUse(volumeHandle(vh))
			if inUseFlag {
				klog.Infof("volume %s is being used by pod %s in pvc %s", vh, podName, pvcName)
				return false
			}
			return true
		}
	}
	klog.Infof("Unable to find volume %s in CNS datastore %s", pv.Name, c.sourceDatastore)
	return false
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

	if err = c.vSphereConnection.Connect(ctx); err != nil {
		return fmt.Errorf("error connecting to vcenter: %v", err)
	}

	return nil
}
