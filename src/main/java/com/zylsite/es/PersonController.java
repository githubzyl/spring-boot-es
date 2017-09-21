package com.zylsite.es;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class PersonController {

	@Autowired
	private TransportClient client;

	@GetMapping("/m/person/man/{id}")
	public Object findById(@PathVariable String id) {
		GetResponse response = client.prepareGet("person", "man", id).get();
		if (!response.isExists()) {
			return new ResponseEntity<>(HttpStatus.NOT_FOUND);
		}
		return new ResponseEntity<>(response.getSource(), HttpStatus.OK);
	}

	@PostMapping("/m/person/man/add")
	public Object add(@RequestParam(name = "name") String name, @RequestParam(name = "country") String country,
			@RequestParam(name = "age") Integer age,
			@RequestParam(name = "birthday") @DateTimeFormat(pattern = "yyyy-MM-dd") Date birthday) {

		try {
			XContentBuilder sourceBuilder = XContentFactory.jsonBuilder().startObject().field("name", name)
					.field("country", country).field("age", age).field("birthday", birthday.getTime()).endObject();
			IndexResponse response = client.prepareIndex("person", "man").setSource(sourceBuilder).get();

			return new ResponseEntity<>(response.getId(), HttpStatus.OK);
		} catch (IOException e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		}

	}

	@DeleteMapping("/m/person/man/remove/{id}")
	public Object remove(@PathVariable String id) {
		DeleteResponse response = client.prepareDelete("person", "man", id).get();
		return new ResponseEntity<>(response.getResult(), HttpStatus.OK);
	}

	@PutMapping("/m/person/man/update")
	public Object update( @RequestParam(name = "id") String id, @RequestParam(name = "name", required = false) String name,
			@RequestParam(name = "country", required = false) String country) {

		UpdateRequest request = new UpdateRequest("person", "man", id);
		try {
			XContentBuilder sourceBuilder = XContentFactory.jsonBuilder().startObject();
			
			if(!StringUtils.isEmpty(name)){
				sourceBuilder.field("name", name);
			}
			if(!StringUtils.isEmpty(country)){
				sourceBuilder.field("country", country);
			}
			
			sourceBuilder.endObject();
			
			request.doc(sourceBuilder);
			
			UpdateResponse response = client.update(request).get();

			return new ResponseEntity<>(response.getResult(), HttpStatus.OK);
		} catch (Exception e) {
			e.printStackTrace();
			return new ResponseEntity<>(HttpStatus.INTERNAL_SERVER_ERROR);
		} 
		
	}
	
	@PostMapping("/m/person/man/query")
	public Object query(@RequestParam(name = "name", required = false) String name,
			@RequestParam(name = "country", required = false) String country,
			@RequestParam(name = "startAge", defaultValue = "0") int startAge,
			@RequestParam(name = "endAge", required = false) Integer endAge){
		
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		
		if(!StringUtils.isEmpty(name)){
			boolQueryBuilder.must(QueryBuilders.matchQuery("name", name));
		}
		if(!StringUtils.isEmpty(country)){
			boolQueryBuilder.must(QueryBuilders.matchQuery("country", country));
		}
		
		RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("age").from(startAge);
		if(null != endAge && 0 < endAge){
			rangeQueryBuilder.to(rangeQueryBuilder);
		}
		
		boolQueryBuilder.filter(rangeQueryBuilder);
		
		SearchRequestBuilder searchRequestBuilder = client.prepareSearch("person")
		      .setTypes("man")
		      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
		      .setQuery(boolQueryBuilder)
		      .setFrom(0)
		      .setSize(10);
		
		System.out.println(searchRequestBuilder);
		
		SearchResponse response = searchRequestBuilder.get();
		List<Map<String,Object>> list = new ArrayList<>();
		for(SearchHit hit : response.getHits()){
			list.add(hit.getSource());
		}
		
		return new ResponseEntity<>(list, HttpStatus.OK);
	}

}
