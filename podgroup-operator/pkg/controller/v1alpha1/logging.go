package v1alpha1

import (
	"fmt"
	"log"
)

func logPGD(pgd *PodGroupDeployment, format string, args ...interface{}) {
	log.Printf("podgroupdeployment=%s/%s %s", pgd.Namespace, pgd.Name, fmt.Sprintf(format, args...))
}

func logPGDR(pgdr *PodGroupDeploymentRollout, format string, args ...interface{}) {
	log.Printf("podgroupdeploymentrollout=%s/%s %s", pgdr.Namespace, pgdr.Name, fmt.Sprintf(format, args...))
}
