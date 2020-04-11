package kubeclient

import (
	"context"
	"fmt"
	"net/http"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	log           = logf.Log.WithName("kubeclient")
	ignoredFields = [...]string{
		"ObjectMeta.SelfLink",
		"ObjectMeta.UID",
		"ObjectMeta.ResourceVersion",
		"ObjectMeta.Generation",
		"ObjectMeta.CreationTimestamp",
		"ObjectMeta.Finalizers",
		"ObjectMeta.ManagedFields",
		"TypeMeta.APIVersion",
	}
	options = cmp.FilterPath(func(path cmp.Path) bool {
		for _, p := range ignoredFields {
			if p == path.String() {
				return true
			}
		}
		return false
	}, cmp.Ignore())
)

type (
	Client struct {
		client.Client
	}
	resource interface {
		runtime.Object
		meta.Object
	}
)

func New(client client.Client) *Client {
	return &Client{Client: client}
}

func (c *Client) Get(key client.ObjectKey, obj runtime.Object) error {
	return c.Client.Get(context.TODO(), key, obj)
}

func (c *Client) Apply(r resource) error {
	kind := r.GetObjectKind().GroupVersionKind().Kind
	namespace := r.GetNamespace()
	name := r.GetName()
	reqLog := log.WithValues("kind", kind, "namespace", namespace, "name", name)

	o := r.DeepCopyObject()
	err := c.Get(client.ObjectKey{Namespace: namespace, Name: name}, o)
	if isNotFound(err) {
		err := c.Client.Create(context.TODO(), r)
		if err != nil {
			return err
		}
		reqLog.Info("resource created")
		return nil
	} else if err != nil {
		return err
	}

	if current, ok := o.(meta.Object); ok {
		r.SetResourceVersion(current.GetResourceVersion())
		diff := cmp.Diff(current, r, options)
		if diff != "" {
			reqLog.Info("resource changes:\n" + diff)
			err = c.Client.Update(context.TODO(), r)
			if err != nil {
				return err
			}
			reqLog.Info("resource updated")
		} else {
			reqLog.Info("resource unchanged")
		}
		return nil
	}

	return fmt.Errorf("invalid object %s", kind)
}

func isNotFound(err error) bool {
	if err == nil {
		return false
	}
	k8sError, ok := err.(*errors.StatusError)
	return ok && k8sError.Status().Code == http.StatusNotFound
}
