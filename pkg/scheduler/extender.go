/*
Copyright 2015 The Kubernetes Authors.

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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	restclient "k8s.io/client-go/rest"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const (
	// DefaultExtenderTimeout defines the default extender timeout in second. 默认的扩展超时时间
	DefaultExtenderTimeout = 5 * time.Second
)

// HTTPExtender implements the Extender interface. HTTPExtender继承Extender接口（kube-scheduler抽象的调度扩展程序的接口），实现了HTTP服务，将Extender的一些接口转换为HTTP请求，所以是调度扩展程序的客户端
type HTTPExtender struct {
	// 调度扩展程序的URL，比如https://127.0.0.1:8080。
	extenderURL string
	// xxxVerb是HTTPExtender.Xxx()接口的HTTP请求的URL，比如https://127.0.0.1:8080/'preemptVerb' 用于ProcessPreemption()接口
	preemptVerb    string
	filterVerb     string
	prioritizeVerb string
	bindVerb       string
	// 调度扩展程序的权重，用来与ScorePlugin计算出最终的分数
	weight int64
	client *http.Client
	// ???【需要删减】调度扩展程序是否缓存了Node信息，如果调度扩展程序已经缓存了集群中所有节点的全部详细信息，那么只需要发送非常少量的Node信息即可，比如Node名字。
	// 毕竟是HTTP调用，想法设法提升效率。但是为什么有podCacheCapable?这就要分析一下HTTPExtender发送的数据包括哪些了？
	// 1. 待调度的Pod
	// 2. Node(候选)
	// 3. 候选Node上的候选Pod（仅抢占调度)
	// 试想一下每次HTTP请求中Pod（包括候选Pod）可能不是不同的，而Node呢？有的请求可能会有不同，但于Filter请求因为需要的是Node全量，所以基本是相同。
	// 会造成较大的无效数据传输，所以当调度扩展程序能够缓存Node信息时，客户端只需要传输很少的信息就可以了。
	nodeCacheCapable bool
	// 调度扩展程序管理的资源名称
	managedResources sets.String
	// 如果调度扩展程序不可用是否忽略
	ignorable bool
}

//返回RoundTripper，RoundTripper是一个接口，代表执行单个HTTP事务的能力，为给定的请求获取响应。
func makeTransport(config *schedulerapi.Extender) (http.RoundTripper, error) {
	var cfg restclient.Config    //定义配置
	if config.TLSConfig != nil { //配置赋值（转移）
		cfg.TLSClientConfig.Insecure = config.TLSConfig.Insecure
		cfg.TLSClientConfig.ServerName = config.TLSConfig.ServerName
		cfg.TLSClientConfig.CertFile = config.TLSConfig.CertFile
		cfg.TLSClientConfig.KeyFile = config.TLSConfig.KeyFile
		cfg.TLSClientConfig.CAFile = config.TLSConfig.CAFile
		cfg.TLSClientConfig.CertData = config.TLSConfig.CertData
		cfg.TLSClientConfig.KeyData = config.TLSConfig.KeyData
		cfg.TLSClientConfig.CAData = config.TLSConfig.CAData
	}
	if config.EnableHTTPS { //若启用HTTPS，检查服务器的受信任根证书
		hasCA := len(cfg.CAFile) > 0 || len(cfg.CAData) > 0
		if !hasCA {
			cfg.Insecure = true //设置为不安全
		}
	}
	tlsConfig, err := restclient.TLSConfigFor(&cfg) //获取tls配置
	if err != nil {
		return nil, err
	}
	if tlsConfig != nil {
		return utilnet.SetTransportDefaults(&http.Transport{ //Transport是支持HTTP、HTTPS和HTTP代理(用于HTTP或使用CONNECT的HTTPS)的RoundTripper的实现。
			TLSClientConfig: tlsConfig, //指定TLSClientConfig的配置为tlsConfig
		}), nil
	}
	return utilnet.SetTransportDefaults(&http.Transport{}), nil
}

// NewHTTPExtender creates an HTTPExtender object. 创建一个HTTPExtender对象
func NewHTTPExtender(config *schedulerapi.Extender) (framework.Extender, error) {
	if config.HTTPTimeout.Duration.Nanoseconds() == 0 { //没有配置超时时间，使用默认超时，5s
		config.HTTPTimeout.Duration = time.Duration(DefaultExtenderTimeout) //设置为默认时间
	}

	transport, err := makeTransport(config) //获取transport，创建http.Client
	if err != nil {
		return nil, err
	}
	client := &http.Client{ //定义http客户端
		Transport: transport,
		Timeout:   config.HTTPTimeout.Duration,
	}
	managedResources := sets.NewString() //管理的资源从slice转为map[string]struct{}
	for _, r := range config.ManagedResources {
		managedResources.Insert(string(r.Name)) //插入r.Name到managedResources
	}
	//配置HTTPExtender的属性
	return &HTTPExtender{
		extenderURL:      config.URLPrefix,
		preemptVerb:      config.PreemptVerb,
		filterVerb:       config.FilterVerb,
		prioritizeVerb:   config.PrioritizeVerb,
		bindVerb:         config.BindVerb,
		weight:           config.Weight,
		client:           client,
		nodeCacheCapable: config.NodeCacheCapable,
		managedResources: managedResources,
		ignorable:        config.Ignorable,
	}, nil
}

// Name returns extenderURL to identify the extender. Name返回extenderURL以标识扩展程序
func (h *HTTPExtender) Name() string {
	return h.extenderURL
}

// IsIgnorable returns true indicates scheduling should not fail when this extender
// is unavailable isignoable返回true表示当扩展器不可用时调度不应该失败
func (h *HTTPExtender) IsIgnorable() bool {
	return h.ignorable
}

// SupportsPreemption returns true if an extender supports preemption.
// An extender should have preempt verb defined and enabled its own node cache. 如果扩展程序支持抢占，SupportsPreemption返回true。扩展程序应该定义了抢占谓词并启用了自己的节点缓存。
func (h *HTTPExtender) SupportsPreemption() bool {
	return len(h.preemptVerb) > 0
}

// ProcessPreemption returns filtered candidate nodes and victims after running preemption logic in extender.ProcessPreemption在扩展器中运行抢占逻辑后返回筛选过的候选节点和受害者。
func (h *HTTPExtender) ProcessPreemption(
	pod *v1.Pod,
	nodeNameToVictims map[string]*extenderv1.Victims,
	nodeInfos framework.NodeInfoLister,
) (map[string]*extenderv1.Victims, error) {
	var (
		result extenderv1.ExtenderPreemptionResult
		args   *extenderv1.ExtenderPreemptionArgs
	)

	if !h.SupportsPreemption() { //如果扩展程序不支持抢占
		return nil, fmt.Errorf("preempt verb is not defined for extender %v but run into ProcessPreemption", h.extenderURL)
	}

	if h.nodeCacheCapable { //如果扩展器缓存了节点信息，在args中传递NodeNameToMetaVictims。
		// If extender has cached node info, pass NodeNameToMetaVictims in args.
		nodeNameToMetaVictims := convertToMetaVictims(nodeNameToVictims) //将nodeNameToVictims从结构类型转换为元类型。
		args = &extenderv1.ExtenderPreemptionArgs{                       //定义扩展参数
			Pod:                   pod,
			NodeNameToMetaVictims: nodeNameToMetaVictims,
		}
	} else {
		args = &extenderv1.ExtenderPreemptionArgs{
			Pod:               pod,
			NodeNameToVictims: nodeNameToVictims,
		}
	}
	//发送信息到扩展
	if err := h.send(h.preemptVerb, args, &result); err != nil {
		return nil, err
	}

	// Extender will always return NodeNameToMetaVictims.
	// So let's convert it to NodeNameToVictims by using <nodeInfos>. Extender总是返回NodeNameToMetaVictims。因此，让我们使用<nodeinfo>将其转换为NodeNameToVictims。
	newNodeNameToVictims, err := h.convertToVictims(result.NodeNameToMetaVictims, nodeInfos)
	if err != nil {
		return nil, err
	}
	// Do not override <nodeNameToVictims>.
	return newNodeNameToVictims, nil
}

// convertToVictims converts "nodeNameToMetaVictims" from object identifiers,
// such as UIDs and names, to object pointers. convertToVictims将“nodeNameToMetaVictims”从对象标识符(如uid和名称)转换为对象指针。
func (h *HTTPExtender) convertToVictims(
	nodeNameToMetaVictims map[string]*extenderv1.MetaVictims,
	nodeInfos framework.NodeInfoLister,
) (map[string]*extenderv1.Victims, error) {
	nodeNameToVictims := map[string]*extenderv1.Victims{}      //定义map key为string，value为*extenderv1.Victims类型
	for nodeName, metaVictims := range nodeNameToMetaVictims { //遍历nodeNameToMetaVictims
		nodeInfo, err := nodeInfos.Get(nodeName) //获取node信息
		if err != nil {                          //若存在错误，返回
			return nil, err
		}
		victims := &extenderv1.Victims{ //声明Victims
			Pods:             []*v1.Pod{},
			NumPDBViolations: metaVictims.NumPDBViolations,
		}
		for _, metaPod := range metaVictims.Pods { //遍历metaVictims的pod并添加到victims中
			pod, err := h.convertPodUIDToPod(metaPod, nodeInfo)
			if err != nil {
				return nil, err
			}
			victims.Pods = append(victims.Pods, pod)
		}
		nodeNameToVictims[nodeName] = victims
	}
	return nodeNameToVictims, nil
}

// convertPodUIDToPod returns v1.Pod object for given MetaPod and node info.
// The v1.Pod object is restored by nodeInfo.Pods().
// It returns an error if there's cache inconsistency between default scheduler
// and extender, i.e. when the pod is not found in nodeInfo.Pods. 返回v1。给定的MetaPod和节点信息。v1。Pod对象由nodeInfo.Pods()恢复。如果默认调度程序和扩展程序之间的缓存不一致，即当在nodeInfo.Pods中找不到pod时，它会返回一个错误。
func (h *HTTPExtender) convertPodUIDToPod(
	metaPod *extenderv1.MetaPod,
	nodeInfo *framework.NodeInfo) (*v1.Pod, error) {
	for _, p := range nodeInfo.Pods { //遍历节点信息中存储的pod信息
		if string(p.Pod.UID) == metaPod.UID { //若节点pod的UID和metaPod的UID一致,表明找到了pod
			return p.Pod, nil
		}
	}
	//在nodeInfo.Pods中找不到pod时
	return nil, fmt.Errorf("extender: %v claims to preempt pod (UID: %v) on node: %v, but the pod is not found on that node",
		h.extenderURL, metaPod, nodeInfo.Node().Name)
}

// convertToMetaVictims converts from struct type to meta types. convertToMetaVictims 从结构类型转换为元类型。
func convertToMetaVictims(
	nodeNameToVictims map[string]*extenderv1.Victims,
) map[string]*extenderv1.MetaVictims {
	nodeNameToMetaVictims := map[string]*extenderv1.MetaVictims{} //定义一个空的map
	for node, victims := range nodeNameToVictims {                //遍历victim的map
		metaVictims := &extenderv1.MetaVictims{ //定义metaVictim对象
			Pods:             []*extenderv1.MetaPod{},
			NumPDBViolations: victims.NumPDBViolations,
		}
		for _, pod := range victims.Pods { //遍历victim中的pods信息并添加到metaVictims中
			metaPod := &extenderv1.MetaPod{
				UID: string(pod.UID),
			}
			metaVictims.Pods = append(metaVictims.Pods, metaPod)
		}
		nodeNameToMetaVictims[node] = metaVictims //加入metaVictims的map
	}
	return nodeNameToMetaVictims
}

// Filter based on extender implemented predicate functions. The filtered list is
// expected to be a subset of the supplied list; otherwise the function returns an error.
// The failedNodes and failedAndUnresolvableNodes optionally contains the list
// of failed nodes and failure reasons, except nodes in the latter are
// unresolvable. 基于扩展器实现的谓词函数进行筛选。过滤后的列表应该是所提供列表的子集;否则，函数将返回错误。failedNodes和failedandunresolvablenode可选地包含失败节点和失败原因的列表，除非后者中的节点是不可解析的。
func (h *HTTPExtender) Filter(
	pod *v1.Pod,
	nodes []*v1.Node,
) (filteredList []*v1.Node, failedNodes, failedAndUnresolvableNodes extenderv1.FailedNodesMap, err error) {
	var (
		result     extenderv1.ExtenderFilterResult
		nodeList   *v1.NodeList
		nodeNames  *[]string
		nodeResult []*v1.Node
		args       *extenderv1.ExtenderArgs
	) //声明变量
	// 将[]*v1.Node转为map[string]*v1.Node，当调度扩展程序缓存了Node信息，返回的结果只有Node名字
	// fromNodeName用于根据Node名字快速查找对应的Node
	fromNodeName := make(map[string]*v1.Node) //创建val为Node的map
	for _, n := range nodes {                 //将nodes中的节点存入fromNodeName
		fromNodeName[n.Name] = n
	}

	if h.filterVerb == "" { //若filterVerb为空，说明没有配置filterVerb，调度扩展程序不支持Filter，直接返回
		return nodes, extenderv1.FailedNodesMap{}, extenderv1.FailedNodesMap{}, nil
	}

	if h.nodeCacheCapable {
		//若HTTPExtender缓存了node信息，则参数中只需要设置Node的名字
		nodeNameSlice := make([]string, 0, len(nodes))
		for _, node := range nodes { //获取nodeName信息
			nodeNameSlice = append(nodeNameSlice, node.Name)
		}
		nodeNames = &nodeNameSlice
	} else {
		//若HTTPExtender没有缓存node信息，把全量的Node放在参数中
		nodeList = &v1.NodeList{}
		for _, node := range nodes { //获取node列表，存储node
			nodeList.Items = append(nodeList.Items, *node)
		}
	}

	args = &extenderv1.ExtenderArgs{ //构造HTTP请求参数
		Pod:       pod,
		Nodes:     nodeList,
		NodeNames: nodeNames,
	}
	//向HTTP扩展h发送请求
	if err := h.send(h.filterVerb, args, &result); err != nil {
		return nil, nil, nil, err
	}
	if result.Error != "" { //若请求有错，返回
		return nil, nil, nil, fmt.Errorf(result.Error)
	}
	//若HTTPExtender扩展缓存了node信息，且结果中设置了Node名字
	if h.nodeCacheCapable && result.NodeNames != nil {
		nodeResult = make([]*v1.Node, len(*result.NodeNames))
		for i, nodeName := range *result.NodeNames {
			// 根据返回结果的Node名字找到Node并输出
			if n, ok := fromNodeName[nodeName]; ok {
				nodeResult[i] = n
			} else {
				return nil, nil, nil, fmt.Errorf(
					"extender %q claims a filtered node %q which is not found in the input node list",
					h.extenderURL, nodeName)
			}
		}
	} else if result.Nodes != nil {
		// 直接从结果中获取Node
		nodeResult = make([]*v1.Node, len(result.Nodes.Items))
		for i := range result.Nodes.Items {
			nodeResult[i] = &result.Nodes.Items[i]
		}
	}

	return nodeResult, result.FailedNodes, result.FailedAndUnresolvableNodes, nil
}

// Prioritize based on extender implemented priority functions. Weight*priority is added
// up for each such priority function. The returned score is added to the score computed
// by Kubernetes scheduler. The total score is used to do the host selection. 基于扩展器实现的优先级函数的优先级。权重*优先级为每个这样的优先级函数相加。返回的分数被添加到Kubernetes调度程序计算的分数中。总得分用于进行主机选择。
func (h *HTTPExtender) Prioritize(pod *v1.Pod, nodes []*v1.Node) (*extenderv1.HostPriorityList, int64, error) {
	var (
		result    extenderv1.HostPriorityList
		nodeList  *v1.NodeList
		nodeNames *[]string
		args      *extenderv1.ExtenderArgs
	)
	// 如果没有配置prioritizeVerb，说明调度扩展程序不支持Prioritize，直接返回
	if h.prioritizeVerb == "" {
		result := extenderv1.HostPriorityList{}
		for _, node := range nodes {
			result = append(result, extenderv1.HostPriority{Host: node.Name, Score: 0})
		}
		return &result, 0, nil
	}

	if h.nodeCacheCapable {
		//若HTTPExtender缓存了node信息，则参数中只需要设置Node的名字
		nodeNameSlice := make([]string, 0, len(nodes))
		for _, node := range nodes {
			nodeNameSlice = append(nodeNameSlice, node.Name)
		}
		nodeNames = &nodeNameSlice
	} else {
		//若HTTPExtender没有缓存node信息，把全量的Node放在参数中
		nodeList = &v1.NodeList{}
		for _, node := range nodes {
			nodeList.Items = append(nodeList.Items, *node)
		}
	}
	//构造HTTP请求参数
	args = &extenderv1.ExtenderArgs{
		Pod:       pod,
		Nodes:     nodeList,
		NodeNames: nodeNames,
	}
	//发送HTTP请求
	if err := h.send(h.prioritizeVerb, args, &result); err != nil {
		return nil, 0, err
	}
	return &result, h.weight, nil
}

// Bind delegates the action of binding a pod to a node to the extender. Bind将将pod绑定到节点的操作委托给扩展程序。
func (h *HTTPExtender) Bind(binding *v1.Binding) error {
	var result extenderv1.ExtenderBindingResult
	//若没有为Bind方法配置该扩展程序，直接返回
	if !h.IsBinder() {
		// This shouldn't happen as this extender wouldn't have become a Binder.
		return fmt.Errorf("unexpected empty bindVerb in extender")
	}
	//构造HTTP请求
	req := &extenderv1.ExtenderBindingArgs{
		PodName:      binding.Name,
		PodNamespace: binding.Namespace,
		PodUID:       binding.UID,
		Node:         binding.Target.Name,
	}
	//发送HTTP请求
	if err := h.send(h.bindVerb, req, &result); err != nil {
		return err
	}
	if result.Error != "" {
		return fmt.Errorf(result.Error)
	}
	return nil
}

// IsBinder returns whether this extender is configured for the Bind method. IsBinder返回是否为Bind方法配置了该扩展程序。
func (h *HTTPExtender) IsBinder() bool {
	return h.bindVerb != ""
}

// Helper function to send messages to the extender
// Helper函数：向extender发送消息
func (h *HTTPExtender) send(action string, args interface{}, result interface{}) error {
	// 将请求参数(比如filter和prioritize请求是ExtenderArgs，preempt请求是ExtenderPreemptionArgs)序列化为JSON格式。
	out, err := json.Marshal(args)
	if err != nil {
		return err
	}
	// 格式化请求的最终URL
	url := strings.TrimRight(h.extenderURL, "/") + "/" + action
	// 创建HTTP请求（POST类型），并将JSON格式的参数放到Body中
	req, err := http.NewRequest("POST", url, bytes.NewReader(out))
	if err != nil {
		return err
	}
	// 设置内容格式是JSON
	req.Header.Set("Content-Type", "application/json")
	// 发送HTTP请求
	resp, err := h.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //延迟执行关闭请求体
	// 检查HTTP的状态码，如果不是200就返回错误
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed %v with extender at URL %v, code %v", action, url, resp.StatusCode)
	}
	// 解析Body中的结果(比如filter请求是ExtenderFilterResult，prioritize请求是HostPriorityList，preempt请求是ExtenderPreemptionResult)
	return json.NewDecoder(resp.Body).Decode(result)
}

// IsInterested returns true if at least one extended resource requested by
// this pod is managed by this extender. 如果这个pod请求的扩展资源由这个扩展程序管理，IsInterested返回true。
func (h *HTTPExtender) IsInterested(pod *v1.Pod) bool {
	//???若HTTPExtender的管理资源为空，说明调度扩展程序对pod感兴趣
	if h.managedResources.Len() == 0 {
		return true
	}
	//判断调度扩展程序是否管理pod的容器的资源
	if h.hasManagedResources(pod.Spec.Containers) {
		return true
	}
	//判断调度扩展程序是否管理pod的初始化容器的资源
	if h.hasManagedResources(pod.Spec.InitContainers) {
		return true
	}
	return false
}

//判断当前调度扩展程序是否有容器的资源管理
func (h *HTTPExtender) hasManagedResources(containers []v1.Container) bool {
	//遍历容器，检查该调度扩展程序h是否管理容器资源
	for i := range containers {
		container := &containers[i]
		//先检查所需的最小计算资源。
		for resourceName := range container.Resources.Requests {
			if h.managedResources.Has(string(resourceName)) {
				return true
			}
		}
		//检查允许使用的计算资源。
		for resourceName := range container.Resources.Limits {
			if h.managedResources.Has(string(resourceName)) {
				return true
			}
		}
	}
	return false
}
