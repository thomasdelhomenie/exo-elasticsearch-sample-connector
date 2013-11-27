package org.exoplatform.search.elasticsearch;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.exoplatform.commons.api.search.SearchServiceConnector;
import org.exoplatform.commons.api.search.data.SearchContext;
import org.exoplatform.commons.api.search.data.SearchResult;
import org.exoplatform.container.xml.InitParams;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.StringWriter;
import java.util.*;

public class ElasticSearchConnector extends SearchServiceConnector {

  private Map<String, String> sortMapping = new HashMap<String, String>();

  public ElasticSearchConnector(InitParams initParams) {
    super(initParams);

    sortMapping.put("date", "title"); // no date field on wikipedia results
    sortMapping.put("relevancy", "_score");
    sortMapping.put("title", "title");
  }

  @Override
  public Collection<SearchResult> search(SearchContext context, String query, Collection<String> sites, int offset, int limit, String sort, String order) {
    Collection<SearchResult> results = new ArrayList<SearchResult>();

    String esQuery = "{\n" +
            "     \"from\" : " + offset + ", \"size\" : " + limit + ",\n" +
            "     \"sort\" : [\n" +
            "       { \"" + sortMapping.get(sort) + "\" : {\"order\" : \"" + order + "\"}}\n" +
            "     ],\n" +
            "     \"query\": {\n" +
            "        \"filtered\" : {\n" +
            "            \"query\" : {\n" +
            "                \"query_string\" : {\n" +
            "                    \"query\" : \"" + query + "\"\n" +
            "                }\n" +
            "            }\n" +
            "        }\n" +
            "     },\n" +
            "     \"highlight\" : {\n" +
            "       \"fields\" : {\n" +
            "         \"text\" : {\"fragment_size\" : 150, \"number_of_fragments\" : 3}\n" +
            "       }\n" +
            "     }\n" +
            "}";


    try {
      HttpClient client = new DefaultHttpClient();
      HttpPost request = new HttpPost("http://localhost:9200/_search");
      StringEntity input = new StringEntity(esQuery);
      request.setEntity(input);

      HttpResponse response = client.execute(request);
      StringWriter writer = new StringWriter();
      IOUtils.copy(response.getEntity().getContent(), writer, "UTF-8");
      String jsonResponse = writer.toString();

      JSONParser parser = new JSONParser();

      Map json = (Map)parser.parse(jsonResponse);
      JSONObject jsonResult = (JSONObject) json.get("hits");
      JSONArray jsonHits = (JSONArray) jsonResult.get("hits");
      for(Object jsonHit : jsonHits) {
        JSONObject hitSource = (JSONObject) ((JSONObject) jsonHit).get("_source");
        String title = (String) hitSource.get("title");
        JSONObject hitHighlights = (JSONObject) ((JSONObject) jsonHit).get("highlight");
        JSONArray hitHighlightsTexts = (JSONArray) hitHighlights.get("text");
        String text = "";
        for(Object hitHighlightsText : hitHighlightsTexts) {
          text += (String) hitHighlightsText + " ... ";
        }

        results.add(new SearchResult(
                "http://wikipedia.org",
                title,
                text,
                "",
                "http://upload.wikimedia.org/wikipedia/commons/thumb/7/77/Wikipedia_svg_logo.svg/45px-Wikipedia_svg_logo.svg.png",
                new Date().getTime(),
                1
        ));
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    return results;
  }
}
