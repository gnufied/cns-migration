package cnsmigration

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

func SayHello() {
	fmt.Println("Hello World")
}

type CNSVolumeMigrator struct {
	clientSet            kubernetes.Interface
	destinationDatastore string
	sourceDatastore      string
}

func NewCNSVolumeMigrator(cs kubernetes.Interface, dsSource, dsTarget string) *CNSVolumeMigrator {
	return &CNSVolumeMigrator{
		clientSet:            cs,
		sourceDatastore:      dsSource,
		destinationDatastore: dsTarget,
	}
}

func (c *CNSVolumeMigrator) StartMigration() error {
	// find existing CSI based persistent volumes, which same datastore as one mentioned in the source
	c.findCSIVolumes()

	// find all persistnet volumes which are not attached to any node.
	// group the persistent volumes by nodes
	return nil
}

func (c *CNSVolumeMigrator) findCSIVolumes() ([]*v1.PersistentVolume, error) {
	pvList, err := c.clientSet.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pv := range pvList.Items {
		fmt.Printf("name of pv is %s\n", pv.Name)
	}
	return nil, nil
}
