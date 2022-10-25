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

package node

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	netutils "k8s.io/utils/net"
)

const (
	// NodeUnreachablePodReason is the reason on a pod when its state cannot be confirmed as kubelet is unresponsive
	// on the node it is (was) running.
	NodeUnreachablePodReason = "NodeLost"
	// NodeUnreachablePodMessage is the message on a pod when its state cannot be confirmed as kubelet is unresponsive
	// on the node it is (was) running.
	NodeUnreachablePodMessage = "Node %v which was running pod %v is unresponsive"
)

// GetHostname returns OS's hostname if 'hostnameOverride' is empty; otherwise, return 'hostnameOverride'.
func GetHostname(hostnameOverride string) (string, error) {
	hostName := hostnameOverride //定义hostName
	if len(hostName) == 0 {      //若hostName为空
		nodeName, err := os.Hostname() //得到操作系统的hostname
		if err != nil {                //error不为空
			return "", fmt.Errorf("couldn't determine hostname: %v", err) //返回为空，打印错误
		}
		hostName = nodeName
	}

	// Trim whitespaces first to avoid getting an empty hostname
	// For linux, the hostname is read from file /proc/sys/kernel/hostname directly
	hostName = strings.TrimSpace(hostName) //删除空白字符
	if len(hostName) == 0 {                //hostName为空
		return "", fmt.Errorf("empty hostname is invalid")
	}
	return strings.ToLower(hostName), nil //返回hostName的小写
}

// NoMatchError is a typed implementation of the error interface. It indicates a failure to get a matching Node.
type NoMatchError struct { //error接口，表示获取匹配的节点失败
	addresses []v1.NodeAddress //节点地址
}

// Error is the implementation of the conventional interface for
// representing an error condition, with the nil value representing no error.
func (e *NoMatchError) Error() string {
	return fmt.Sprintf("no preferred addresses found; known addresses: %v", e.addresses) //返回字符串，输出error的地址
}

// GetPreferredNodeAddress returns the address of the provided node, using the provided preference order.
// If none of the preferred address types are found, an error is returned.
func GetPreferredNodeAddress(node *v1.Node, preferredAddressTypes []v1.NodeAddressType) (string, error) {
	for _, addressType := range preferredAddressTypes { //遍历首选地址类型
		for _, address := range node.Status.Addresses {
			if address.Type == addressType { //若类型与首选地址类型匹配，返回结果
				return address.Address, nil
			}
		}
	}
	return "", &NoMatchError{addresses: node.Status.Addresses} //返回error，表示获取匹配的节点失败
}

// GetNodeHostIPs returns the provided node's IP(s); either a single "primary IP" for the
// node in a single-stack cluster, or a dual-stack pair of IPs in a dual-stack cluster
// (for nodes that actually have dual-stack IPs). Among other things, the IPs returned
// from this function are used as the `.status.PodIPs` values for host-network pods on the
// node, and the first IP is used as the `.status.HostIP` for all pods on the node.
func GetNodeHostIPs(node *v1.Node) ([]net.IP, error) {
	// Re-sort the addresses with InternalIPs first and then ExternalIPs
	allIPs := make([]net.IP, 0, len(node.Status.Addresses)) //定义IP空数组，预留内存空间大小为node.Status.Addresses的长度
	for _, addr := range node.Status.Addresses {            //遍历节点地址
		if addr.Type == v1.NodeInternalIP { //若节点地址类型为NodeInternalIP
			ip := netutils.ParseIPSloppy(addr.Address) //解析地址，获取Ip
			if ip != nil {                             //若ip不为空
				allIPs = append(allIPs, ip) //将所得ip加入Ip数组
			}
		}
	}
	for _, addr := range node.Status.Addresses { //遍历节点地址
		if addr.Type == v1.NodeExternalIP { //若节点地址类型为NodeExternalIP
			ip := netutils.ParseIPSloppy(addr.Address) //解析地址，获取Ip
			if ip != nil {                             //若ip不为空
				allIPs = append(allIPs, ip) //将所得ip加入Ip数组
			}
		}
	}
	if len(allIPs) == 0 { //若IP数组为空
		return nil, fmt.Errorf("host IP unknown; known addresses: %v", node.Status.Addresses) //返回为空+报错信息
	}

	nodeIPs := []net.IP{allIPs[0]} //定义nodeIps,包含allIPs中的第一个Ip地址
	for _, ip := range allIPs {    //遍历allIPs
		if netutils.IsIPv6(ip) != netutils.IsIPv6(nodeIPs[0]) { //若当前ip与nodeIps[0]类型不同
			nodeIPs = append(nodeIPs, ip) //将ip插入nodeIps
			break                         //跳出循环
		}
	}
	//返回节点的Ip，单栈集群中节点ip，或双栈集群中的双栈ip对（Ipv6格式和Ipv4格式）
	return nodeIPs, nil
}

// GetNodeHostIP returns the provided node's "primary" IP; see GetNodeHostIPs for more details
func GetNodeHostIP(node *v1.Node) (net.IP, error) { //返回节点的hostIp
	ips, err := GetNodeHostIPs(node)
	if err != nil {
		return nil, err
	}
	// GetNodeHostIPs always returns at least one IP if it didn't return an error
	return ips[0], nil
}

// GetNodeIP returns an IP (as with GetNodeHostIP) for the node with the provided name.
// If required, it will wait for the node to be created.
func GetNodeIP(client clientset.Interface, name string) net.IP { //按照名字返回nodeIp
	var nodeIP net.IP
	backoff := wait.Backoff{ //定义backoff（回退）参数
		Steps:    6,
		Duration: 1 * time.Second,
		Factor:   2.0,
		Jitter:   0.2,
	}
	//ExponentialBackoff-使用指数回退重复条件检查
	err := wait.ExponentialBackoff(backoff, func() (bool, error) {
		node, err := client.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil {
			klog.Errorf("Failed to retrieve node info: %v", err)
			return false, nil
		}
		nodeIP, err = GetNodeHostIP(node) //得到节点的hostIp
		if err != nil {
			klog.Errorf("Failed to retrieve node IP: %v", err)
			return false, err
		}
		return true, nil
	})
	if err == nil {
		klog.Infof("Successfully retrieved node IP: %v", nodeIP)
	}
	return nodeIP //返回nodeIp
}

// IsNodeReady returns true if a node is ready; false otherwise.
func IsNodeReady(node *v1.Node) bool { //判断节点是否准备
	for _, c := range node.Status.Conditions { //遍历节点的状态
		if c.Type == v1.NodeReady { //状态类型为NodeReady
			return c.Status == v1.ConditionTrue //返回判断结果
		}
	}
	return false
}
