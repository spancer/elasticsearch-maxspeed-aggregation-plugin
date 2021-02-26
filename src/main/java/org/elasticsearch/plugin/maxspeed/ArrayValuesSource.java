package org.elasticsearch.plugin.maxspeed;

import java.util.Map;

import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ArrayValuesSource<Time extends ValuesSource, Geo extends ValuesSource> {
  private final static Logger logger = LoggerFactory.getLogger(ArrayValuesSource.class);

  protected MultiValueMode multiValueMode;
  protected String[] names;
  protected Time time;
  protected Geo geo;

  public static class MultArrayValuesSource
      extends ArrayValuesSource<ValuesSource.Numeric, ValuesSource.GeoPoint> {

    public MultArrayValuesSource(Map<String, ValuesSource> valuesSources,
        MultiValueMode multiValueMode) {
      super(valuesSources, multiValueMode);
      if (valuesSources != null) {
        for (Map.Entry<String, ValuesSource> item : valuesSources.entrySet()) {

          if (item.getValue() instanceof ValuesSource.Numeric == true) {
            this.setTime((ValuesSource.Numeric) item.getValue());
          } else if (item.getValue() instanceof ValuesSource.GeoPoint == true) {
            this.setGeo((ValuesSource.GeoPoint) item.getValue());
          } else {
            logger.info("只支持数字类型和Geo_Point类型");
          }
        }

      } else {
        this.time = ValuesSource.Numeric.EMPTY;
        this.geo = ValuesSource.GeoPoint.EMPTY;
      }
    }
  }

  private ArrayValuesSource(Map<String, ?> valuesSources, MultiValueMode multiValueMode) {
    if (valuesSources != null) {
      this.names = valuesSources.keySet().toArray(new String[0]);
    }
    this.multiValueMode = multiValueMode;
  }


  public boolean needsScores() {
    return false;
  }

  public String[] fieldNames() {
    return this.names;
  }


  public Time getTime() {
    return time;
  }


  public void setTime(Time time) {
    this.time = time;
  }


  public Geo getGeo() {
    return geo;
  }


  public void setGeo(Geo geo) {
    this.geo = geo;
  }


}
