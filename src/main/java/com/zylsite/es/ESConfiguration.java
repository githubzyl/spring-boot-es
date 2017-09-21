package com.zylsite.es;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ESConfiguration {

	@Bean
	public TransportClient client() throws UnknownHostException{
		//配置节点
		InetSocketTransportAddress node = new InetSocketTransportAddress(
				InetAddress.getByName("192.168.0.105"), 9300);
		
		//配置集群名称
		Settings settings = Settings.builder()
				.put("cluster.name","jason")
				.build();
		
		TransportClient client = new PreBuiltTransportClient(settings);
		client.addTransportAddress(node);
		//可添加多个...
		
		return client;
	}
	
}
