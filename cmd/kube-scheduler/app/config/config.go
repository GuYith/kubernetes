/*
Copyright 2018 The Kubernetes Authors.

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

package config

import (
	"time"

	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

// Config has all the context to run a Scheduler Config拥有运行Scheduler所需的所有上下文
type Config struct {
	// ComponentConfig is the scheduler server's configuration object. 调度程序服务器的配置对象
	ComponentConfig kubeschedulerconfig.KubeSchedulerConfiguration

	// LoopbackClientConfig is a config for a privileged loopback connection 特权环回连接的配置
	LoopbackClientConfig *restclient.Config

	Authentication apiserver.AuthenticationInfo //身份认证信息
	Authorization  apiserver.AuthorizationInfo  //验证信息
	SecureServing  *apiserver.SecureServingInfo //安全服务信息

	Client             clientset.Interface                          //客户端接口
	KubeConfig         *restclient.Config                           //rest客户端
	InformerFactory    informers.SharedInformerFactory              //informer的工厂类
	DynInformerFactory dynamicinformer.DynamicSharedInformerFactory //动态informer的工厂类

	//nolint:staticcheck // SA1019 this deprecated field still needs to be used for now. It will be removed once the migration is done.
	EventBroadcaster events.EventBroadcasterAdapter //EventBroadcaster适配器

	// LeaderElection is optional.
	LeaderElection *leaderelection.LeaderElectionConfig //领导人选取配置

	// PodMaxInUnschedulablePodsDuration is the maximum time a pod can stay in
	// unschedulablePods. If a pod stays in unschedulablePods for longer than this
	// value, the pod will be moved from unschedulablePods to backoffQ or activeQ.
	// If this value is empty, the default value (5min) will be used.
	PodMaxInUnschedulablePodsDuration time.Duration
}

type completedConfig struct {
	*Config
}

// CompletedConfig same as Config, just to swap private object.
type CompletedConfig struct { //转变为私有对象
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
func (c *Config) Complete() CompletedConfig {
	cc := completedConfig{c}

	apiserver.AuthorizeClientBearerToken(c.LoopbackClientConfig, &c.Authentication, &c.Authorization) //??? 补全令牌

	return CompletedConfig{&cc}
}
