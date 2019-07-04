package wang.huaiyu.druid.web;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import io.druid.java.util.common.granularity.GranularityType;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.Druids;
import io.druid.query.LegacyDataSource;
import io.druid.query.QueryDataSource;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.ListFilteredDimensionSpec;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.having.GreaterThanHavingSpec;
import io.druid.query.groupby.having.LessThanHavingSpec;
import io.druid.query.metadata.metadata.ListColumnIncluderator;
import io.druid.query.metadata.metadata.SegmentAnalysis;
import io.druid.query.metadata.metadata.SegmentMetadataQuery;
import io.druid.query.ordering.StringComparators;
import io.druid.query.scan.ScanQuery;
import io.druid.query.scan.ScanResultValue;
import io.druid.query.search.SearchQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.SearchSortSpec;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import io.druid.query.spec.LegacySegmentSpec;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ValueType;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.Interval;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import wang.huaiyu.druid.core.DruidExecutor;
import wang.huaiyu.druid.core.data.Result;

import java.util.*;

/**
 * 查询控制器
 *
 * @author wang.huaiyu
 * @date 2019-06-20
 */
@RestController
public class DemoQueryController {

    private DruidExecutor druidExecutor;

    @Autowired
    public DemoQueryController(DruidExecutor druidExecutor) {
        this.druidExecutor = druidExecutor;
    }

    /**
     * 搜索(select count(*) from table)
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("search")
    public String search() {
        Druids.SearchQueryBuilder builder = new Druids.SearchQueryBuilder();
        SearchQuery query = builder.dataSource("wikipedia")
                // 细粒度
                .granularity(GranularityType.ALL.getDefaultGranularity())
                // 时间范围
                .intervals("2016-01-01/2016-12-12")
                // 查询维度
                .dimensions(Collections.singletonList(DefaultDimensionSpec.of("flags")))
                // 过滤条件
                .filters("channel", "#ca.wikipedia")
                // 排序
                .sortSpec(new SearchSortSpec(new StringComparators.NumericComparator()))
                // 查询数量 default 1000
                .limit(1000)
                .build();
        Result<SearchResultValue> result = druidExecutor.get(query, SearchResultValue.class);
        return JSON.toJSONString(result);
    }

    /**
     * 查询(select * from table)
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("select")
    public String select() {
        Druids.SelectQueryBuilder builder = new Druids.SelectQueryBuilder();

        // 分页时需要同条件下上一次查询的 pagingIdentifiers
        Map<String, Integer> pagingIdentifiers = new HashMap<>(4);
        pagingIdentifiers.put("wikipedia_2016-06-27T00:00:00.000Z_2016-06-28T00:00:00.000Z_2019-06-19T10:51:08.434Z", 1);

        SelectQuery query = builder.dataSource("wikipedia")
                .granularity(GranularityType.ALL.getDefaultGranularity())
                .intervals("2016-01-01/2016-12-12")
                // 降序 default false // TODO 不知道干啥的 反正false就对了
                .descending(false)
                // 查询维度 default all
//                .dimensions()
                // 查询度量 default all
//                .metrics()
                // 分页
                .pagingSpec(new PagingSpec(pagingIdentifiers, 2))
                .build();
        Result<SelectResultValue> result = druidExecutor.get(query, SelectResultValue.class);
        return JSON.toJSONString(result);
    }

    /**
     * 扫描(select * from table limit 0, ?)基本没什么用
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("scan")
    public String scan() {
        ScanQuery.ScanQueryBuilder builder = new ScanQuery.ScanQueryBuilder();
        ScanQuery query = builder.dataSource("wikipedia")
                .intervals(new MultipleIntervalSegmentSpec(Collections.singletonList(Interval.parse("2016-01-01/2016-12-12"))))
                .limit(10)
                .build();
        Result<ScanResultValue> result = druidExecutor.get(query, ScanResultValue.class);
        return JSON.toJSONString(result);
    }

    /**
     * top
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("top")
    public String top() {
        Map<String, Object> context = new HashMap<>(3);
        context.put("minTopNThreshold", 1000);
        TopNQueryBuilder builder = new TopNQueryBuilder();
        TopNQuery query = builder.dataSource("wikipedia")
                .intervals("2016-01-01/2016-12-12")
                .granularity(GranularityType.ALL.getDefaultGranularity())
                .dimension("channel", "out_channel")
                // 度量标准
                .metric("channel")
                .threshold(25)
                .aggregators(Collections.singletonList(new CountAggregatorFactory("channel")))
//                .aggregators(Collections.singletonList(new CardinalityAggregatorFactory("channel", Collections.singletonList(new DefaultDimensionSpec("channel", "channel")), true)))
//                .postAggregators(Collections.singletonList())
                .context(context)
                .build();
        Result<TopNResultValue> result = druidExecutor.get(query, TopNResultValue.class);
        return JSON.toJSONString(result);
    }

    /**
     * 分段元数据查询
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("query")
    public String query() {
        Druids.SegmentMetadataQueryBuilder builder = new Druids.SegmentMetadataQueryBuilder();
        SegmentMetadataQuery query = builder.dataSource("wikipedia")
                // 时间范围
                .intervals("2016-01-01/2016-12-12")
                .toInclude(new ListColumnIncluderator(Collections.singletonList("flags")))
                .build();
        Result<SegmentAnalysis> result = druidExecutor.get(query, SegmentAnalysis.class);
        return JSON.toJSONString(result);
    }

    /**
     * 时间序列化查询(去重)
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("series")
    public String series() {
        TimeseriesQuery query = new TimeseriesQuery(
                new LegacyDataSource("wikipedia"),
                new LegacySegmentSpec("2016-01-01/2016-12-12"),
                true,
                null,
//                new BoundDimFilter("added", "2", "3", false, true, null, null, StringComparators.NUMERIC),
                null,
                GranularityType.HOUR.getDefaultGranularity(),
                Arrays.asList(
                        new CardinalityAggregatorFactory("distinctAdded", Collections.singletonList(DefaultDimensionSpec.of("added")), true),
                        new CountAggregatorFactory("added"),
                        new LongSumAggregatorFactory("sum_added", "added")
                ),
                null,
                null);
        List<Result<TimeseriesResultValue>> result = druidExecutor.find(query, TimeseriesResultValue.class);
        return JSON.toJSONString(result);
    }

    /**
     * 分组查询
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("group")
    public String group() {
        GroupByQuery.Builder builder = GroupByQuery.builder();
        GroupByQuery query = builder
                .setDataSource("wikipedia")
                .setInterval("2016-01-01/2016-12-12")
                .setGranularity(GranularityType.ALL.getDefaultGranularity())
//                .addDimension("flags", "groupFlags")
                .addDimension(new ListFilteredDimensionSpec(new DefaultDimensionSpec("City Name", "cityName"), new HashSet<>(Arrays.asList("Rome", "Tokyo")), true))
                .addAggregator(new CountAggregatorFactory("City Name"))
//                .addAggregator(new CardinalityAggregatorFactory("distinctCityName", Collections.singletonList(DefaultDimensionSpec.of("City Name")), true))
//                .setPostAggregatorSpecs(
//                        Collections.singletonList(
//                                new ArithmeticPostAggregator(
//                                        "aggregatorFlags",
//                                        "+",
//                                        Arrays.asList(
//                                                new FieldAccessPostAggregator("test1", "flags"),
//                                                new FieldAccessPostAggregator("test2", "added")
//                                        )
//                                )
//                        )
//                )
                .build();
        List<Result<JSONObject>> result = druidExecutor.find(query, JSONObject.class);
        return JSON.toJSONString(result);
    }

    /**
     * 子查询
     *
     * @return 结果 JSON 字符串
     */
    @GetMapping("sub")
    public String sub() {
        GroupByQuery subQuery = GroupByQuery.builder()
                .setDataSource("wikipedia")
                .setInterval("2016-01-01/2016-12-12")
                .setGranularity(GranularityType.ALL.getDefaultGranularity())
                .addDimension("channel", "group_channel")
                .addAggregator(new CountAggregatorFactory("added"))
                .setHavingSpec(new LessThanHavingSpec("added", 100))
                .build();
        GroupByQuery outQuery = GroupByQuery.builder()
                .setDataSource(subQuery)
                .setInterval("2016-01-01/2016-12-12")
                .setGranularity(GranularityType.ALL.getDefaultGranularity())
                .addDimension("added", "added")
                .addAggregator(new CountAggregatorFactory("group_channel"))
                .build();
        List<Result<JSONObject>> result = druidExecutor.find(outQuery, JSONObject.class);
        return JSON.toJSONString(result);
    }
}