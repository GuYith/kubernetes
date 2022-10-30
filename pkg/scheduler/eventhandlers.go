/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"fmt"
	"reflect"
	"strings"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	corev1nodeaffinity "k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeaffinity"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodename"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/nodeports"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/noderesources"
	"k8s.io/kubernetes/pkg/scheduler/internal/queue"
	"k8s.io/kubernetes/pkg/scheduler/profile"
)

//onStorageClassAdd是kube-scheduler处理StorageClass的Added事件的函数
func (sched *Scheduler) onStorageClassAdd(obj interface{}) {
	//获取StorageClass
	sc, ok := obj.(*storagev1.StorageClass)
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *storagev1.StorageClass", "obj", obj)
		return
	}

	// CheckVolumeBindingPred fails if pod has unbound immediate PVCs. If these
	// PVCs have specified StorageClass name, creating StorageClass objects
	// with late binding will cause predicates to pass, so we need to move pods
	// to active queue.
	// We don't need to invalidate cached results because results will not be
	// cached for pod that has unbound immediate PVCs.

	//PVC: PersistentVolumeClaim持久卷
	//如果pod有未绑定的immediate pvc, CheckVolumeBindingPred失败。如果这些pvc已经指定了StorageClass名称，那么创建带有后期绑定的StorageClass对象将导致谓词传递，因此我们需要将pod移动到active队列。我们不需要使缓存的结果无效，因为对于有未绑定的immediate pvc的pod，结果不会被缓存。
	if sc.VolumeBindingMode != nil && *sc.VolumeBindingMode == storagev1.VolumeBindingWaitForFirstConsumer {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.StorageClassAdd, nil)
	}
}

//addNodeToCache()是kube-scheduler处理Node的Added事件的函数。比如，集群管理员向集群添加了Node
func (sched *Scheduler) addNodeToCache(obj interface{}) {
	node, ok := obj.(*v1.Node) //获取node
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", obj)
		return
	}
	//在调度器的缓存中添加节点
	nodeInfo := sched.Cache.AddNode(node)
	//打印日志
	klog.V(3).InfoS("Add event for node", "node", klog.KObj(node))
	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.NodeAdd, preCheckForNode(nodeInfo))
}

//updateNodeInCache()是kube-scheduler处理Node的Updated事件的函数。比如，集群管理员为Node打了标签
func (sched *Scheduler) updateNodeInCache(oldObj, newObj interface{}) {
	oldNode, ok := oldObj.(*v1.Node) //获取旧对象的节点
	if !ok {
		klog.ErrorS(nil, "Cannot convert oldObj to *v1.Node", "oldObj", oldObj)
		return
	}
	newNode, ok := newObj.(*v1.Node) //定义新对象的节点
	if !ok {
		klog.ErrorS(nil, "Cannot convert newObj to *v1.Node", "newObj", newObj)
		return
	}
	//更新缓存中的节点信息
	nodeInfo := sched.Cache.UpdateNode(oldNode, newNode)
	// Only requeue unschedulable pods if the node became more schedulable. 只有当节点变得更可调度时，才会请求不可调度的pod。
	//更可调度：比如可分配资源增加、以前不可调度现在可调度等
	//nodeSchedulingPropertiesChange判断可调度属性是否发生变化
	if event := nodeSchedulingPropertiesChange(newNode, oldNode); event != nil { //若发生变化，移动pod
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(*event, preCheckForNode(nodeInfo))
	}
}

// deleteNodeFromCache()是kube-scheduler处理Node的Deleted事件的函数。比如，集群管理员将Node从集群中删除
func (sched *Scheduler) deleteNodeFromCache(obj interface{}) {
	var node *v1.Node //声明node
	switch t := obj.(type) {
	case *v1.Node: //若obj为node类型，直接获取node
		node = t
	case cache.DeletedFinalStateUnknown: //若obj为cache.DeletedFinalStateUnknown类型（处于删除状态），取出其中的node
		var ok bool
		node, ok = t.Obj.(*v1.Node)
		if !ok {
			klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", t.Obj)
			return
		}
	default:
		klog.ErrorS(nil, "Cannot convert to *v1.Node", "obj", t)
		return
	}
	klog.V(3).InfoS("Delete event for node", "node", klog.KObj(node))
	//将node从调度缓存中移除
	if err := sched.Cache.RemoveNode(node); err != nil {
		klog.ErrorS(err, "Scheduler cache RemoveNode failed")
	}
}

//addPodToSchedulingQueue()是kube-scheduler处理未调度Pod的添加了事件的函数。比如通过kubectl创建的Pod，控制器创建的Pod等等
func (sched *Scheduler) addPodToSchedulingQueue(obj interface{}) {
	pod := obj.(*v1.Pod)
	klog.V(3).InfoS("Add event for unscheduled pod", "pod", klog.KObj(pod))
	if err := sched.SchedulingQueue.Add(pod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to queue %T: %v", obj, err))
	}
}

// updatePodInSchedulingQueue()是kube-scheduler处理未调度Pod的Updated事件的函数。
// 还处在调度队列中Pod被更新，比如需更新资源需求、亲和性、标签等
func (sched *Scheduler) updatePodInSchedulingQueue(oldObj, newObj interface{}) {
	oldPod, newPod := oldObj.(*v1.Pod), newObj.(*v1.Pod) //定义新旧pod
	// Bypass update event that carries identical objects; otherwise, a duplicated
	// Pod may go through scheduling and cause unexpected behavior (see #96071). 绕过携带相同对象的更新事件;否则，重复的Pod可能会通过调度并导致意外行为
	if oldPod.ResourceVersion == newPod.ResourceVersion {
		return
	}
	//获取pod是否过期
	isAssumed, err := sched.Cache.IsAssumedPod(newPod)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to check whether pod %s/%s is assumed: %v", newPod.Namespace, newPod.Name, err))
	}
	if isAssumed {
		return
	}
	//若pod没有过期，更新调度队列中的旧pod
	if err := sched.SchedulingQueue.Update(oldPod, newPod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to update %T: %v", newObj, err))
	}
}

// deletePodFromSchedulingQueue()是kube-scheduler处理未调度Pod的Deleted事件的函数。
func (sched *Scheduler) deletePodFromSchedulingQueue(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) { //判断对象类型，获取pod
	case *v1.Pod:
		pod = obj.(*v1.Pod)
	case cache.DeletedFinalStateUnknown: //若pod处于删除状态
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
			return
		}
	default:
		utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
		return
	}
	klog.V(3).InfoS("Delete event for unscheduled pod", "pod", klog.KObj(pod))
	//从调度队列删除pod
	if err := sched.SchedulingQueue.Delete(pod); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to dequeue %T: %v", obj, err))
	}
	// 根据Pod.Spec.SchedulerName获取调度框架(调度器)
	fwk, err := sched.frameworkForPod(pod)
	if err != nil {
		// This shouldn't happen, because we only accept for scheduling the pods
		// which specify a scheduler name that matches one of the profiles.
		klog.ErrorS(err, "Unable to get profile", "pod", klog.KObj(pod))
		return
	}
	// If a waiting pod is rejected, it indicates it's previously assumed and we're
	// removing it from the scheduler cache. In this case, signal a AssignedPodDelete
	// event to immediately retry some unscheduled Pods. 如果一个等待pod被拒绝，这表示它是先前被假定调度的，我们正在从调度程序缓存中删除它。在这种情况下，发送一个assignnedpoddelete事件来立即重试一些未调度的Pods。
	if fwk.RejectWaitingPod(pod.UID) {
		sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.AssignedPodDelete, nil)
	}
}

// addPodToCache()是kube-scheduler处理已调度Pod的Added事件的函数
func (sched *Scheduler) addPodToCache(obj interface{}) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", obj)
		return
	}
	klog.V(3).InfoS("Add event for scheduled pod", "pod", klog.KObj(pod))
	//将Pod添加到调度缓存中
	if err := sched.Cache.AddPod(pod); err != nil {
		klog.ErrorS(err, "Scheduler cache AddPod failed", "pod", klog.KObj(pod))
	}
	//???触发调度队列更新可能依赖于此Pod的其他Pod的调度状态。
	sched.SchedulingQueue.AssignedPodAdded(pod)
}

// updatePodInCache()是kube-scheduler处理已调度Pod的Updated事件的函数。
func (sched *Scheduler) updatePodInCache(oldObj, newObj interface{}) {
	oldPod, ok := oldObj.(*v1.Pod)
	if !ok {
		klog.ErrorS(nil, "Cannot convert oldObj to *v1.Pod", "oldObj", oldObj)
		return
	}
	newPod, ok := newObj.(*v1.Pod) //声明新pod
	if !ok {
		klog.ErrorS(nil, "Cannot convert newObj to *v1.Pod", "newObj", newObj)
		return
	}
	klog.V(4).InfoS("Update event for scheduled pod", "pod", klog.KObj(oldPod))

	// 更新调度缓存中的pod
	if err := sched.Cache.UpdatePod(oldPod, newPod); err != nil {
		klog.ErrorS(err, "Scheduler cache UpdatePod failed", "pod", klog.KObj(oldPod))
	}
	//触发调度队列更新可能依赖于此Pod的其他Pod的调度状态
	sched.SchedulingQueue.AssignedPodUpdated(newPod)
}

// deletePodFromCache()是kube-scheduler处理已调度Pod的Deleted事件的函数。
func (sched *Scheduler) deletePodFromCache(obj interface{}) {
	var pod *v1.Pod
	switch t := obj.(type) { //判断对象的类型，获取pod
	case *v1.Pod:
		pod = t
	case cache.DeletedFinalStateUnknown:
		var ok bool
		pod, ok = t.Obj.(*v1.Pod)
		if !ok {
			klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", t.Obj)
			return
		}
	default:
		klog.ErrorS(nil, "Cannot convert to *v1.Pod", "obj", t)
		return
	}
	klog.V(3).InfoS("Delete event for scheduled pod", "pod", klog.KObj(pod))
	//从调度器缓存移除pod
	if err := sched.Cache.RemovePod(pod); err != nil {
		klog.ErrorS(err, "Scheduler cache RemovePod failed", "pod", klog.KObj(pod))
	}

	sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.AssignedPodDelete, nil)
}

// assignedPod selects pods that are assigned (scheduled and running). 判断是否是assigned pod（已调度或运行中）
func assignedPod(pod *v1.Pod) bool {
	return len(pod.Spec.NodeName) != 0
}

// responsibleForPod returns true if the pod has asked to be scheduled by the given scheduler. 如果pod请求由给定的调度程序调度，则responleforpod返回true。
func responsibleForPod(pod *v1.Pod, profiles profile.Map) bool {
	return profiles.HandlesSchedulerName(pod.Spec.SchedulerName)
}

// addAllEventHandlers is a helper function used in tests and in Scheduler
// to add event handlers for various informers. 在测试和调度器中用于为各种通知程序添加（注册）事件处理程序的助手函数
func addAllEventHandlers(
	sched *Scheduler, //sched是调度器对象
	// kube-scheduler所有模块共享使用的SharedIndexInformer工厂,DynamicSharedInformer工厂
	informerFactory informers.SharedInformerFactory,
	dynInformerFactory dynamicinformer.DynamicSharedInformerFactory,
	gvkMap map[framework.GVK]framework.ActionType,
) {
	// scheduled pod cache 已调度pod缓存
	// 注册已调度Pod事件处理函数
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			// 事件过滤函数
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				/// 如果对象是Pod，只有已调度的Pod才会通过过滤条件
				case *v1.Pod:
					return assignedPod(t)
				case cache.DeletedFinalStateUnknown: //pod处于已删除状态
					if _, ok := t.Obj.(*v1.Pod); ok {
						// The carried object may be stale, so we don't use it to check if
						// it's assigned or not. Attempting to cleanup anyways.
						return true
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			// 注册已调度Pod事件处理函数，配置相关方法
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToCache,
				UpdateFunc: sched.updatePodInCache,
				DeleteFunc: sched.deletePodFromCache,
			},
		},
	)
	// unscheduled pod queue
	// 注册未调度Pod事件处理函数，根据Pod的状态过滤未调度的Pod
	informerFactory.Core().V1().Pods().Informer().AddEventHandler(
		cache.FilteringResourceEventHandler{
			//过滤函数
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Pod:
					//若pod没有被调度，且pod请求由给定的调度程序调度
					return !assignedPod(t) && responsibleForPod(t, sched.Profiles)
				case cache.DeletedFinalStateUnknown: //pod处于删除状态
					if pod, ok := t.Obj.(*v1.Pod); ok {
						// The carried object may be stale, so we don't use it to check if
						// it's assigned or not.
						return responsibleForPod(pod, sched.Profiles)
					}
					utilruntime.HandleError(fmt.Errorf("unable to convert object %T to *v1.Pod in %T", obj, sched))
					return false
				default:
					utilruntime.HandleError(fmt.Errorf("unable to handle object in %T: %T", sched, obj))
					return false
				}
			},
			// 注册未调度Pod事件处理函数
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    sched.addPodToSchedulingQueue,
				UpdateFunc: sched.updatePodInSchedulingQueue,
				DeleteFunc: sched.deletePodFromSchedulingQueue,
			},
		},
	)
	// 注册Node事件处理函数
	informerFactory.Core().V1().Nodes().Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    sched.addNodeToCache,
			UpdateFunc: sched.updateNodeInCache,
			DeleteFunc: sched.deleteNodeFromCache,
		},
	)
	//???
	buildEvtResHandler := func(at framework.ActionType, gvk framework.GVK, shortGVK string) cache.ResourceEventHandlerFuncs {
		funcs := cache.ResourceEventHandlerFuncs{}
		if at&framework.Add != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Add, Label: fmt.Sprintf("%vAdd", shortGVK)}
			funcs.AddFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
			}
		}
		if at&framework.Update != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Update, Label: fmt.Sprintf("%vUpdate", shortGVK)}
			funcs.UpdateFunc = func(_, _ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
			}
		}
		if at&framework.Delete != 0 {
			evt := framework.ClusterEvent{Resource: gvk, ActionType: framework.Delete, Label: fmt.Sprintf("%vDelete", shortGVK)}
			funcs.DeleteFunc = func(_ interface{}) {
				sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(evt, nil)
			}
		}
		return funcs
	}
	// group version kind
	for gvk, at := range gvkMap { //遍历
		switch gvk { //判断gvk类型
		case framework.Node, framework.Pod:
			// Do nothing.
		case framework.CSINode:
			//注册CSINode事件处理函数
			//CSINode：CSINode保存节点上安装的所有CSI驱动程序的信息。
			informerFactory.Storage().V1().CSINodes().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.CSINode, "CSINode"),
			)
		case framework.CSIDriver:
			//注册CSIDrivers事件处理函数
			informerFactory.Storage().V1().CSIDrivers().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.CSIDriver, "CSIDriver"),
			)
		case framework.CSIStorageCapacity:
			//注册CSIStorageCapacity事件处理函数
			informerFactory.Storage().V1().CSIStorageCapacities().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.CSIStorageCapacity, "CSIStorageCapacity"),
			)
		case framework.PersistentVolume:
			// MaxPDVolumeCountPredicate: since it relies on the counts of PV.
			//
			// PvAdd: Pods created when there are no PVs available will be stuck in
			// unschedulable queue. But unbound PVs created for static provisioning and
			// delay binding storage class are skipped in PV controller dynamic
			// provisioning and binding process, will not trigger events to schedule pod
			// again. So we need to move pods to active queue on PV add for this
			// scenario.
			//
			// PvUpdate: Scheduler.bindVolumesWorker may fail to update assumed pod volume
			// bindings due to conflicts if PVs are updated by PV controller or other
			// parties, then scheduler will add pod back to unschedulable queue. We
			// need to move pods to active queue on PV update for this scenario.
			//注册PersistentVolumes事件处理函数
			informerFactory.Core().V1().PersistentVolumes().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.PersistentVolume, "Pv"),
			)
		case framework.PersistentVolumeClaim:
			// MaxPDVolumeCountPredicate: add/update PVC will affect counts of PV when it is bound.
			//注册PersistentVolumeClaims事件处理函数
			informerFactory.Core().V1().PersistentVolumeClaims().Informer().AddEventHandler(
				buildEvtResHandler(at, framework.PersistentVolumeClaim, "Pvc"),
			)
		case framework.StorageClass:
			//framework.Add为ActionTypes常量
			//at&framework.Add != 0 表明当前的action中有add
			if at&framework.Add != 0 {
				//注册StorageClasses事件处理函数，
				informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
					//资源事件处理函数类型为add
					cache.ResourceEventHandlerFuncs{
						AddFunc: sched.onStorageClassAdd,
					},
				)
			}
			//表明当前的action中有update
			if at&framework.Update != 0 {
				//注册StorageClasses事件处理函数，update
				informerFactory.Storage().V1().StorageClasses().Informer().AddEventHandler(
					cache.ResourceEventHandlerFuncs{
						UpdateFunc: func(_, _ interface{}) {
							sched.SchedulingQueue.MoveAllToActiveOrBackoffQueue(queue.StorageClassUpdate, nil)
						},
					},
				)
			}
		default:
			// Tests may not instantiate dynInformerFactory.
			if dynInformerFactory == nil {
				continue
			}
			// GVK is expected to be at least 3-folded, separated by dots.
			// <kind in plural>.<version>.<group>
			// Valid examples:
			// - foos.v1.example.com
			// - bars.v1beta1.a.b.c
			// Invalid examples:
			// - foos.v1 (2 sections)
			// - foo.v1.example.com (the first section should be plural)
			if strings.Count(string(gvk), ".") < 2 {
				klog.ErrorS(nil, "incorrect event registration", "gvk", gvk)
				continue
			}
			// Fall back to try dynamic informers.
			// gvr：group、version、resource
			gvr, _ := schema.ParseResourceArg(string(gvk))
			dynInformer := dynInformerFactory.ForResource(*gvr).Informer()
			//dynInformer，注册事件处理函数
			dynInformer.AddEventHandler(
				buildEvtResHandler(at, gvk, strings.Title(gvr.Resource)),
			)
		}
	}
}

// nodeSchedulingPropertiesChange判断可调度属性是否发生变化
func nodeSchedulingPropertiesChange(newNode *v1.Node, oldNode *v1.Node) *framework.ClusterEvent {
	//判断Node.Spec.Unschedulable是否发生变化
	if nodeSpecUnschedulableChanged(newNode, oldNode) {
		return &queue.NodeSpecUnschedulableChange
	}
	//判断Node.Status.Allocatable是否发生变化
	if nodeAllocatableChanged(newNode, oldNode) {
		return &queue.NodeAllocatableChange
	}
	//判断Node.Labels是否发生变化
	if nodeLabelsChanged(newNode, oldNode) {
		return &queue.NodeLabelChange
	}
	//判断Node.Spec.Taints是否发生变化
	if nodeTaintsChanged(newNode, oldNode) {
		return &queue.NodeTaintChange
	}
	//判断Node.Status.Conditions是否发生变化
	if nodeConditionsChanged(newNode, oldNode) {
		return &queue.NodeConditionChange
	}

	return nil
}

//判断节点可分配的变化
func nodeAllocatableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.Status.Allocatable, newNode.Status.Allocatable)
}

//判断节点标签的变化
func nodeLabelsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(oldNode.GetLabels(), newNode.GetLabels())
}

//判断节点污点的变化
//Taints：污点，是Node的一个属性，设置了Taints(污点)后，因为有了污点，所以Kubernetes是不会将Pod调度到这个Node上的
func nodeTaintsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return !reflect.DeepEqual(newNode.Spec.Taints, oldNode.Spec.Taints)
}

//判断节点状态的变化
func nodeConditionsChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	strip := func(conditions []v1.NodeCondition) map[v1.NodeConditionType]v1.ConditionStatus {
		conditionStatuses := make(map[v1.NodeConditionType]v1.ConditionStatus, len(conditions))
		for i := range conditions {
			conditionStatuses[conditions[i].Type] = conditions[i].Status
		}
		return conditionStatuses
	}
	return !reflect.DeepEqual(strip(oldNode.Status.Conditions), strip(newNode.Status.Conditions))
}

//判断节点的spec的不可调度的变化
func nodeSpecUnschedulableChanged(newNode *v1.Node, oldNode *v1.Node) bool {
	return newNode.Spec.Unschedulable != oldNode.Spec.Unschedulable && !newNode.Spec.Unschedulable
}

//节点的先前检查
func preCheckForNode(nodeInfo *framework.NodeInfo) queue.PreEnqueueCheck {
	// Note: the following checks doesn't take preemption into considerations, in very rare
	// cases (e.g., node resizing), "pod" may still fail a check but preemption helps. We deliberately
	// chose to ignore those cases as unschedulable pods will be re-queued eventually. 注意:下面的检查不考虑抢占，在非常罕见的情况下(例如，节点调整大小)，“pod”可能仍然会失败，但抢占是有帮助的。我们故意选择忽略这些情况，因为无法调度的舱最终会重新排队。
	return func(pod *v1.Pod) bool {
		admissionResults := AdmissionCheck(pod, nodeInfo, false)
		if len(admissionResults) != 0 {
			return false
		}
		_, isUntolerated := corev1helpers.FindMatchingUntoleratedTaint(nodeInfo.Node().Spec.Taints, pod.Spec.Tolerations, func(t *v1.Taint) bool {
			return t.Effect == v1.TaintEffectNoSchedule
		})
		return !isUntolerated
	}
}

// AdmissionCheck calls the filtering logic of noderesources/nodeport/nodeAffinity/nodename
// and returns the failure reasons. It's used in kubelet(pkg/kubelet/lifecycle/predicate.go) and scheduler.
// It returns the first failure if `includeAllFailures` is set to false; otherwise
// returns all failures. AdmissionCheck调用noderresources /nodeport/nodeAffinity/nodename的过滤逻辑，并返回失败原因。
// 录用检查，对调度器能否接受pod进行检查，AdmissionResult中存储失败原因
func AdmissionCheck(pod *v1.Pod, nodeInfo *framework.NodeInfo, includeAllFailures bool) []AdmissionResult {
	var admissionResults []AdmissionResult //定义录用结果
	//不足的资源数组
	insufficientResources := noderesources.Fits(pod, nodeInfo)
	//将不足的资源添加入AdmissionResult
	if len(insufficientResources) != 0 {
		for i := range insufficientResources {
			admissionResults = append(admissionResults, AdmissionResult{InsufficientResource: &insufficientResources[i]})
		}
		if !includeAllFailures {
			return admissionResults
		}
	}
	//获取所需的节点亲和性，若为false，添加该失败原因到admissionResults
	if matches, _ := corev1nodeaffinity.GetRequiredNodeAffinity(pod).Match(nodeInfo.Node()); !matches {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodeaffinity.Name, Reason: nodeaffinity.ErrReasonPod})
		if !includeAllFailures {
			return admissionResults
		}
	}
	//若pod和节点不匹配，添加失败原因
	if !nodename.Fits(pod, nodeInfo) {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodename.Name, Reason: nodename.ErrReason})
		if !includeAllFailures {
			return admissionResults
		}
	}
	//若pod与节点端口不匹配，添加失败原因
	if !nodeports.Fits(pod, nodeInfo) {
		admissionResults = append(admissionResults, AdmissionResult{Name: nodeports.Name, Reason: nodeports.ErrReason})
		if !includeAllFailures {
			return admissionResults
		}
	}
	return admissionResults
}

// AdmissionResult describes the reason why Scheduler can't admit the pod.
// If the reason is a resource fit one, then AdmissionResult.InsufficientResource includes the details. AdmissionResult描述了为什么调度器不能接收pod的原因。如果原因是资源匹配，则AdmissionResult。InsufficientResource包含详细信息。
type AdmissionResult struct {
	Name                 string
	Reason               string
	InsufficientResource *noderesources.InsufficientResource
}
