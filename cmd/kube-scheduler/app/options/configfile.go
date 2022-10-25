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

package options

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	configv1 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1"
	configv1beta2 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1beta2"
	configv1beta3 "k8s.io/kubernetes/pkg/scheduler/apis/config/v1beta3"
)

//从文件中加载配置信息
func loadConfigFromFile(file string) (*config.KubeSchedulerConfiguration, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}

	return loadConfig(data)
}

func loadConfig(data []byte) (*config.KubeSchedulerConfiguration, error) {
	// The UniversalDecoder runs defaulting and returns the internal type by default.
	//解析data，返回对象、gvk（groupVersionKind)、error
	obj, gvk, err := scheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	if cfgObj, ok := obj.(*config.KubeSchedulerConfiguration); ok { //if ok为true
		// We don't set this field in pkg/scheduler/apis/config/{version}/conversion.go
		// because the field will be cleared later by API machinery during
		// conversion. See KubeSchedulerConfiguration internal type definition for
		// more details.
		cfgObj.TypeMeta.APIVersion = gvk.GroupVersion().String()
		switch cfgObj.TypeMeta.APIVersion {
		case configv1beta2.SchemeGroupVersion.String():
			klog.InfoS("KubeSchedulerConfiguration v1beta2 is deprecated in v1.25, will be removed in v1.28")
		case configv1beta3.SchemeGroupVersion.String():
			klog.InfoS("KubeSchedulerConfiguration v1beta3 is deprecated in v1.26, will be removed in v1.29")
		}
		return cfgObj, nil
	}
	return nil, fmt.Errorf("couldn't decode as KubeSchedulerConfiguration, got %s: ", gvk)
}

//编码配置
func encodeConfig(cfg *config.KubeSchedulerConfiguration) (*bytes.Buffer, error) {
	buf := new(bytes.Buffer)
	const mediaType = runtime.ContentTypeYAML //获取媒体类型
	info, ok := runtime.SerializerInfoForMediaType(scheme.Codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return buf, fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}

	var encoder runtime.Encoder //声明编码器，按照cfg.TypeMeta.APIVersion类型赋值
	switch cfg.TypeMeta.APIVersion {
	case configv1beta2.SchemeGroupVersion.String():
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1beta2.SchemeGroupVersion)
	case configv1beta3.SchemeGroupVersion.String():
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1beta3.SchemeGroupVersion)
	case configv1.SchemeGroupVersion.String():
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1.SchemeGroupVersion)
	default:
		encoder = scheme.Codecs.EncoderForVersion(info.Serializer, configv1.SchemeGroupVersion)
	}
	if err := encoder.Encode(cfg, buf); err != nil { //编码并返回编码结果
		return buf, err
	}
	return buf, nil
}

// LogOrWriteConfig logs the completed component config and writes it into the given file name as YAML, if either is enabled 记录完成的组件配置，并将其作为YAML写入给定的文件名(如果启用了其中任何一个的话)
func LogOrWriteConfig(fileName string, cfg *config.KubeSchedulerConfiguration, completedProfiles []config.KubeSchedulerProfile) error {
	klogV := klog.V(2)
	if !klogV.Enabled() && len(fileName) == 0 { //判断是否启用klog
		return nil
	}
	cfg.Profiles = completedProfiles //配置文件

	buf, err := encodeConfig(cfg) //编码配置
	if err != nil {
		return err
	}

	if klogV.Enabled() {
		klogV.InfoS("Using component config", "config", buf.String())
	}

	if len(fileName) > 0 { //若filename不为空
		configFile, err := os.Create(fileName) //创建文件
		if err != nil {
			return err
		}
		defer configFile.Close() //延迟调用栈，结束时关闭文件
		if _, err := io.Copy(configFile, buf); err != nil {
			return err
		}
		klog.InfoS("Wrote configuration", "file", fileName)
		os.Exit(0)
	}
	return nil
}
