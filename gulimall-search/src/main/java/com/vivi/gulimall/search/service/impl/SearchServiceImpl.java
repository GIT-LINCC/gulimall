package com.vivi.gulimall.search.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.*;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.*;
import co.elastic.clients.json.JsonData;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.vivi.common.constant.SearchConstant;
import com.vivi.common.to.BrandTO;
import com.vivi.common.to.SkuESModel;
import com.vivi.common.utils.R;
import com.vivi.gulimall.search.config.ProductSearchConfig;
import com.vivi.gulimall.search.feign.ProductFeignService;
import com.vivi.gulimall.search.service.SearchService;
import com.vivi.gulimall.search.vo.AttrRespVO;
import com.vivi.gulimall.search.vo.SearchParam;
import com.vivi.gulimall.search.vo.SearchResult;
import lombok.extern.slf4j.Slf4j;
//import org.elasticsearch.action.search.SearchRequest;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.nested.ParsedNested;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedLongTerms;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author wangwei
 * 2020/10/28 16:52
 */
@Slf4j
@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    private ElasticsearchClient esClient;

//    @Autowired
//    private RestHighLevelClient esClient2;

    @Autowired
    private ProductFeignService productFeignService;

    @Override
    public SearchResult search(SearchParam param) {
//        SearchRequest searchRequest = buildSearchRequest(param);
        SearchResult result = null;
        try {
//            SearchResponse<SkuESModel> response = esClient.search(searchRequest, SkuESModel.class);
//            SearchResponse response2 = esClient2.search(searchRequest, RequestOptions.DEFAULT);
//            result = buildSearchResult(param, response);
              result=buildSearchResponse(param);
        } catch (IOException e) {
            log.error("检索ES失败: {}", e);
        }
        return result;
    }


    /**
     * 从前端传来的查询参数构建出 DSL 去ES进行查询
     * <p>
     * 模糊匹配keyword，
     * 过滤(分类id。品牌id，价格区间，是否有库存，规格属性)，
     * 排序，
     * 分页，
     * 高亮，
     * 聚合分析
     * <p>
     * GET /gulimall-product/_search
     * {
     * "query": {
     * "bool": {
     * "must": [
     * {} # keyword模糊匹配
     * ],
     * "filter": [
     * {}, 分类id
     * {}, 品牌id
     * {}, 价格区间
     * {}, 是否有库存，
     * {} 规格属性
     * ]
     * }
     * },
     * "sort": [
     * {}       排序
     * ],
     * "from": 0,  分页
     * "size": 2,
     * "highlight": {}  高亮
     * "aggs": {}   聚合分析
     * }
     *
     * @param param
     * @return
     */
//    private SearchRequest buildSearchRequest(SearchParam param) {
//
//        SearchRequest searchRequest = new SearchRequest();
//
//        // 指定索引
//        searchRequest.indices(SearchConstant.ESIndex.ES_PRODUCT_INDEX);
//        //新版java api使用建造者创建对象并且直接指定索引
////        SearchRequest.Builder searchRequest=new SearchRequest.Builder().index(SearchConstant.ESIndex.ES_PRODUCT_INDEX);
//        // 构建搜索条件
////        SearchSourceBuilder builder = new SearchSourceBuilder();

//        // 构建bool查询
//        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
////        BoolQuery.Builder boolQueryBuilder = new BoolQuery.Builder();
//        // 1.模糊匹配keyword
//        if (!StringUtils.isEmpty(param.getKeyword())) {
//            boolQueryBuilder.must(QueryBuilders.matchQuery("skuTitle", param.getKeyword()));
////            boolQueryBuilder.must(m -> m.match(mm -> mm.field("skuTitle").query(param.getKeyword())));
//        }
//        // 2.过滤(分类id。品牌id，价格区间，是否有库存，规格属性)，
//        // 2.1 分类id
//        if (param.getCatelog3Id() != null &&  param.getCatelog3Id() > 0) {
//            boolQueryBuilder.filter(QueryBuilders.termQuery("catelogId", param.getCatelog3Id()));
////            boolQueryBuilder.filter(f -> f.term(t -> t.field("catelogId").value(param.getCatelog3Id())));
//        }
//        // 2.2 品牌id
//        List<Long> brandId = param.getBrandId();
//        if (!CollectionUtils.isEmpty(brandId)) {
//            boolQueryBuilder.filter(QueryBuilders.termsQuery("brandId", brandId));
////            boolQueryBuilder.filter(f->f.term(t->t.field("brandId").value(brandId)));
//        }
//        // 2.3 价格区间 1_500 / _500 / 500_
//        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery("skuPrice");
////        RangeQuery.Builder rangeQueryBuilder = new RangeQuery.Builder().field("skuPrice");
//        String price = param.getSkuPrice();
//        if (!StringUtils.isEmpty(price)) {
//            String[] priceInfo = price.split("_");
//            // 1_500
//            if (priceInfo.length == 2) {
//                rangeQueryBuilder.gte(JsonData.of(priceInfo[0])).lte(JsonData.of(priceInfo[1]));
//            //    _500
//            } else if (price.startsWith("_")) {
//                rangeQueryBuilder.lte(JsonData.of(priceInfo[0]));
//            //    500_
//            } else {
//                rangeQueryBuilder.gte(JsonData.of(priceInfo[0]));
//            }
//        }
//        boolQueryBuilder.filter(rangeQueryBuilder);
////        boolQueryBuilder.filter(f -> f.range(rangeQueryBuilder.build()));
//        // 2.4 库存
//        if (param.getHasStock() != null) {
//            boolean flag = param.getHasStock() == 0 ? false : true;
//            boolQueryBuilder.filter(QueryBuilders.termQuery("hasStock", flag));
////            boolQueryBuilder.filter(f -> f.term(t -> t.field("hasStock").value(flag)));
//        }
//        // 2.5 规格属性
//        // attrs=1_钢精:铝合&attrs=2_anzhuo:apple&attrs=3_lisi ==> attrs=[1_钢精:铝合,2_anzhuo:apple,3_lisi]
//        List<String> attrs = param.getAttrs();
//        if (!CollectionUtils.isEmpty(attrs)) {
//            // 每个属性参数 attrs=1_钢精:铝合 ==》 nestedQueryFilter
//            /**
//             *          {
//             *           "nested": {
//             *             "path": "",
//             *             "query": {
//             *               "bool": {
//             *                 "must": [
//             *                   {},
//             *                   {}
//             *                 ]
//             *               }
//             *             }
//             *           }
//             *         },
//             */
//            for (String attr : attrs) {
//                String[] attrInfo = attr.split("_");
//                BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
//                boolQuery.must(QueryBuilders.termQuery("attrs.attrId", attrInfo[0]));
//                boolQuery.must(QueryBuilders.termsQuery("attrs.attrValue", attrInfo[1].split(":")));
//                NestedQueryBuilder nestedQueryBuilder = QueryBuilders.nestedQuery("attrs", boolQuery, ScoreMode.None);
//                boolQueryBuilder.filter(nestedQueryBuilder);
//            }
////            for (String attr : attrs) {
////                String[] attrInfo = attr.split("_");
////                BoolQuery.Builder boolQuery = new BoolQuery.Builder();
////                boolQuery.must(m -> m.term(t -> t.field("attrs.attrId").value(attrInfo[0])));
////                boolQuery.must(m -> m.terms(t -> t.field("attrs.attrValue").terms(ts -> ts.value(attrInfo[1].split(":")))));
////                NestedQuery.Builder nestedQueryBuilder = new NestedQuery.Builder().path("attrs").query(boolQuery.build()._toQuery());
////                boolQueryBuilder.filter(f -> f.nested(nestedQueryBuilder.build()));
////            }
//        }
//
//        // 第一部分bool查询组合结束
////        searchRequest.query(boolQueryBuilder);
//        searchRequest.query(boolQueryBuilder.build()._toQuery());
//
//        // 3.排序，sort=hotScore_asc/desc
//        String sortStr = param.getSort();
//        if (!StringUtils.isEmpty(sortStr)) {
//            String[] sortInfo = sortStr.split("_");
//            SortOrder sortType = sortInfo[1].equalsIgnoreCase("asc") ? SortOrder.ASC : SortOrder.DESC;
//            builder.sort(sortInfo[0], sortType);
////            searchRequest.sort(s -> s.field(f -> f.field(sortInfo[0]).order(sortType)));
//        }
//
//        // 4.分页，
//        builder.from(param.getPageNum() == null ? 0 : (param.getPageNum() - 1) * ProductSearchConfig.PAGE_SIZE);
//        builder.size(ProductSearchConfig.PAGE_SIZE);
//
////        searchRequest.from(param.getPageNum() == null ? 0 : (param.getPageNum() - 1) * ProductSearchConfig.PAGE_SIZE);
////        searchRequest.size(ProductSearchConfig.PAGE_SIZE);
//        // 5.高亮，查询关键字不为空才有结果高亮
//        if (!StringUtils.isEmpty(param.getKeyword())) {
//            HighlightBuilder highlightBuilder = new HighlightBuilder();
//            highlightBuilder.field("skuTitle").preTags("<b style='color:red'>").postTags("</b>");
//            builder.highlighter(highlightBuilder);
//
////            Highlight.Builder highlightBuilder = new Highlight.Builder().fields("skuTitle", h -> h.preTags("<b style='color:red'>").postTags("</b>"));
////            searchRequest.highlight(highlightBuilder.build());
//        }
//        // 6.聚合分析，分析得到的商品所涉及到的分类、品牌、规格参数，
//        // term值的是分布情况，就是存在哪些值，每种值下有几个数据; size是取所有结果的前几种，(按id聚合后肯定是同一种，所以可以指定为1)
//        // 6.1 分类部分，按照分类id聚合，划分出分类后，每个分类内按照分类名字聚合就得到分类名，不用再根据id再去查询数据库
//        TermsAggregationBuilder catelogAgg = AggregationBuilders.terms("catelogAgg").field("catelogId");
//        catelogAgg.subAggregation(AggregationBuilders.terms("catelogNameAgg").field("catelogName").size(1));
//        builder.aggregation(catelogAgg);
//        // 6.2 品牌部分，按照品牌id聚合，划分出品牌后，每个品牌内按照品牌名字聚合就得到品牌名，不用再根据id再去查询数据库
//        // 每个品牌内按照品牌logo聚合就得到品牌logo，不用再根据id再去查询数据库
//        TermsAggregationBuilder brandAgg = AggregationBuilders.terms("brandAgg").field("brandId");
//        brandAgg.subAggregation(AggregationBuilders.terms("brandNameAgg").field("brandName").size(1));
//        brandAgg.subAggregation(AggregationBuilders.terms("brandImgAgg").field("brandImg").size(1));
//        builder.aggregation(brandAgg);
//        // 6.3 规格参数部分，按照规格参数id聚合，划分出规格参数后，每个品牌内按照规格参数名字聚合就得到规格参数名，不用再根据id再去查询数据库
//        // 每个规格参数内按照规格参数值聚合就得到规格参数值，不用再根据id再去查询数据库
//        NestedAggregationBuilder nestedAggregationBuilder = AggregationBuilders.nested("attrAgg", "attrs");
//        TermsAggregationBuilder attrIdAgg = AggregationBuilders.terms("attrIdAgg").field("attrs.attrId");
//        attrIdAgg.subAggregation(AggregationBuilders.terms("attrNameAgg").field("attrs.attrName").size(1));
//        attrIdAgg.subAggregation(AggregationBuilders.terms("attrValueAgg").field("attrs.attrValue"));
//        nestedAggregationBuilder.subAggregation(attrIdAgg);
//        builder.aggregation(nestedAggregationBuilder);
//
//        // 组和完成
//        System.out.println("搜索参数构建的DSL语句：" + builder);
//        searchRequest.source(searchRequest.build().source());
//        return searchRequest.build();
//    }
    private SearchResult buildSearchResponse(SearchParam param) throws IOException {
        // 定义搜索关键词
        SearchResponse<SkuESModel> response = getSkuESModelSearchResponse(param);

        SearchResult result = new SearchResult();
        HitsMetadata<SkuESModel> hitsMetadata = response.hits();
        TotalHits totalHits = hitsMetadata.total();
        // 处理命中数据
        List<SkuESModel> esModels = hitsMetadata.hits().stream().map(hit->{
            SkuESModel esModel=hit.source();
            if (!StringUtils.isEmpty(param.getKeyword())) {
            String skuTitle = hit.highlight().get("skuTitle").get(0);
            esModel.setSkuTitle(skuTitle);
            }
            return esModel;
        }).collect(Collectors.toList());
        result.setSkuList(esModels);


        //        /**
//         * 聚合结果--分类
//         */
//        Aggregations aggregations = response.getAggregations();
//        // debug模式下确定这个返回的具体类型
//        ParsedLongTerms catelogAgg = aggregations.get("catelogAgg");
//        // 每一个bucket是一种分类，有几个bucket就会有几个分类
        HistogramAggregate catelogAgg2=response.aggregations().get("catelogAgg").histogram();
        List<SearchResult.CatelogVO> catelogs = catelogAgg2.buckets().array().stream().map(bucket -> {
            // debug查看下结果
            long catelogId = (long) bucket.key();
            // debug模式下确定这个返回的具体类型
            StringTermsAggregate catelogNameAgg2 = bucket.aggregations().get("catelogNameAgg").sterms();
            // 根据id分类后肯定是同一类，只可能有一种名字，所以直接取第一个bucket
//            String catelogName = catelogNameAgg.getBuckets().get(0).getKeyAsString();
            String catelogName = catelogNameAgg2.buckets().array().get(0).key();
            SearchResult.CatelogVO catelogVO = new SearchResult.CatelogVO();
            catelogVO.setCatelogId(catelogId);
            catelogVO.setCatelogName(catelogName);
            return catelogVO;
        }).collect(Collectors.toList());
        result.setCatelogs(catelogs);
//        /**
//         * 聚合结果--品牌，与上面过程类似
//         */
        LongTermsAggregate brandAgg2=response.aggregations().get("brandAgg").lterms();
        List<SearchResult.BrandVO> brands = brandAgg2.buckets().array().stream().map(bucket -> {
            long brandId2 = bucket.key();
            StringTermsAggregate brandNameAgg2 = bucket.aggregations().get("brandNameAgg").sterms();
            String brandName = brandNameAgg2.buckets().array().get(0).key();
            StringTermsAggregate brandImgAgg = bucket.aggregations().get("brandImgAgg").sterms();
            String brandImg = brandImgAgg.buckets().array().get(0).key();
            SearchResult.BrandVO brandVO = new SearchResult.BrandVO();
            brandVO.setBrandId(brandId2);
            brandVO.setBrandName(brandName);
            brandVO.setBrandImg(brandImg);
            return brandVO;
        }).collect(Collectors.toList());
        result.setBrands(brands);
                /**
         * 聚合结果--规格参数
         */
        NestedAggregate attrAgg = response.aggregations().get("attrAgg").nested();
        LongTermsAggregate attrIdAgg = attrAgg.aggregations().get("attrIdAgg").lterms();
        List<SearchResult.AttrVO> attrs = attrIdAgg.buckets().array().stream().map(bucket -> {
            long attrId = bucket.key();
            StringTermsAggregate attrNameAgg = bucket.aggregations().get("attrNameAgg").sterms();
            // 根据id分类后肯定是同一类，只可能有一种名字，所以直接取第一个bucket
            String attrName = attrNameAgg.buckets().array().get(0).key();
            // 根据id分类后肯定是同一类，但是可以有多个值，所以会有多个bucket，把所有值组合起来
            StringTermsAggregate attrValueAgg = bucket.aggregations().get("attrValueAgg").sterms();
            List<String> attrValue = attrValueAgg.buckets().array().stream().map(b -> b.key()).collect(Collectors.toList());
            SearchResult.AttrVO attrVO = new SearchResult.AttrVO();
            attrVO.setAttrId(attrId);
            attrVO.setAttrName(attrName);
            attrVO.setAttrValue(attrValue);
            return attrVO;
        }).collect(Collectors.toList());
        result.setAttrs(attrs);

        /**
         * 分页信息
         */
        // 总记录数
        result.setTotalCount(totalHits.value());
        // 每页大小
        result.setPageSize(ProductSearchConfig.PAGE_SIZE);
        // 总页数
        result.setTotalPage((result.getTotalCount() + ProductSearchConfig.PAGE_SIZE - 1) / ProductSearchConfig.PAGE_SIZE);
        // 当前页码
        int pageNum = param.getPageNum() == null ? 1 : param.getPageNum();
        result.setCurrPage(pageNum);
        // 构建页码导航,以当前页为中心，连续5页
        ArrayList<Integer> pageNavs = new ArrayList<>();
        for (int i = pageNum - 2; i <= pageNum + 2; ++i) {
            if (i <= 0) {
                continue;
            }
            if (i >= result.getTotalPage()) {
                break;
            }
            pageNavs.add(i);
        }
        result.setPageNavs(pageNavs);

        List<SearchResult.BreadCrumbsVO> breadCrumbsVOS = new LinkedList<>();
        /**
         * 构建面包屑导航--参数品牌部分
         */
        List<Long> ids = param.getBrandId();
        if (!CollectionUtils.isEmpty(ids)) {
            R res = productFeignService.getBatch(ids);
            if (res.getCode() == 0) {
                List<BrandTO> brandTOS = res.getData(new TypeReference<List<BrandTO>>() {});
                brandTOS.forEach(brandTO -> {
                    SearchResult.BreadCrumbsVO crumb = new SearchResult.BreadCrumbsVO();
                    crumb.setAttrName("品牌");
                    crumb.setAttrValue(brandTO.getName());
                    // 请求参数中去掉当前属性之后的链接地址
                    String link = param.getQueryString().replace("&brandId=" + brandTO.getBrandId(), "").replace("brandId=" + brandTO.getBrandId(), "");
                    crumb.setLink("http://search.gulimall.com/list.html?" + link);
                    breadCrumbsVOS.add(crumb);
                });
            } else {
                log.warn("ESSearch调用gulimall-product/brand/info/batch失败");
            }
        }
        List<String> queryAttrs = param.getAttrs();
        if (!CollectionUtils.isEmpty(queryAttrs)) {
            List<SearchResult.BreadCrumbsVO> crumbsVOS = queryAttrs.stream().map(attrStr -> {
                // id_value
                String[] attrInfo = attrStr.split("_");
                SearchResult.BreadCrumbsVO breadCrumbsVO = new SearchResult.BreadCrumbsVO();
                breadCrumbsVO.setAttrValue(attrInfo[1]);
                // 请求参数中去掉当前属性之后的链接地址
                String link = "";
                try {
                    // request对路径进行了编码。我们得先把自己的参数编码。才能在路径正正确匹配并替换
                    String encode = URLEncoder.encode(attrStr, "utf-8");
                    // java编码后，空格会被替换为 + ，而浏览器会编码为 %20；英文()会被编码成%28,%29，而浏览器不会编码英文()
                    // 所以我们还得把+替换为浏览器的规则
                    encode = encode.replace("+", "%20").replace("%28", "(").replace("%29", ")");
                    // 去掉 &attrs=1_陶瓷
                    URLDecoder.decode(param.getQueryString(), "iso-8859-1");
                    link = param.getQueryString().replace("&attrs=" + encode, "").replace("attrs=" + encode, "");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                breadCrumbsVO.setLink("http://search.gulimall.com/list.html?" + link);
                // 保存请求参数中的attrId
                result.getParamAttrIds().add(Long.parseLong(attrInfo[0]));
                // 远程调用
                try {
                    R r = productFeignService.info(Long.valueOf(attrInfo[0]));
                    if (r.getCode() == 0) {
                        AttrRespVO attrRespVO = r.getData("attr", AttrRespVO.class);
                        breadCrumbsVO.setAttrName(attrRespVO.getAttrName());
                    }
                } catch (Exception e) {
                    log.error("gulimall-search调用gulimall-product根据attrId查询attrInfo失败：{}", e);
                }
                return breadCrumbsVO;
            }).collect(Collectors.toList());
            breadCrumbsVOS.addAll(crumbsVOS);

        }
        // 保存所有面包屑
        result.setBreadCrumbsNavs(breadCrumbsVOS);
        return result;
    }

    private SearchResponse<SkuESModel> getSkuESModelSearchResponse(SearchParam param) throws IOException {
        String searchText = param.getKeyword();

// 创建查询条件列表
        List<Query> mustQueries = new ArrayList<>();

// 根据产品名称搜索
        if (!StringUtils.isEmpty(searchText)) {
            Query byName = MatchQuery.of(m -> m
                    .field("skuTitle")
                    .query(searchText)
            )._toQuery();
            mustQueries.add(byName);
        }

// 创建过滤条件列表
        List<Query> filterQueries = new ArrayList<>();

// 过滤分类id
        if (param.getCatelog3Id() != null && param.getCatelog3Id() > 0) {
            Query byCatelogId = TermQuery.of(t -> t
                    .field("catelogId")
                    .value(param.getCatelog3Id())
            )._toQuery();
            filterQueries.add(byCatelogId);
        }

// 过滤品牌id
        List<Long> brandId = param.getBrandId();
        if (!CollectionUtils.isEmpty(brandId)) {
            Query byBrandId = TermsQuery.of(t -> t
                    .field("brandId")
                    .terms(ts -> ts.value(brandId.stream().map(FieldValue::of).collect(Collectors.toList())))
            )._toQuery();
            filterQueries.add(byBrandId);
        }
        // 2.3 价格区间 1_500 / _500 / 500_
        String price = param.getSkuPrice();
        Query rangeQuery = RangeQuery.of(t -> {
            t.field("skuPrice");
            if (!StringUtils.isEmpty(price)) {
                String[] priceInfo = price.split("_");
                // 1_500
                if (priceInfo.length == 2) {
                    t.gte(JsonData.of(priceInfo[0])).lte(JsonData.of(priceInfo[1]));
                    // _500
                } else if (price.startsWith("_")) {
                    t.lte(JsonData.of(priceInfo[0]));
                    // 500_
                } else {
                    t.gte(JsonData.of(priceInfo[0]));
                }
            }
            return t;
        })._toQuery();
        filterQueries.add(rangeQuery);


//        // 2.4 库存
        if (param.getHasStock() != null) {
            boolean flag = param.getHasStock() == 0 ? false : true;
            Query byStock = TermQuery.of(t -> t
                    .field("hasStock")
                    .value(flag)
            )._toQuery();
            filterQueries.add(byStock);
        }

        // 3.排序，sort=hotScore_asc/desc
        List<SortOptions> sortOptions = new ArrayList<>();
        String sortStr = param.getSort();
        if (!StringUtils.isEmpty(sortStr)) {
            String[] sortInfo = sortStr.split("_");
            SortOrder sortType = sortInfo[1].equalsIgnoreCase("asc") ? SortOrder.Asc : SortOrder.Desc;
            FieldSort fieldSort = FieldSort.of(f -> f
                    .field(sortInfo[0])
                    .order(sortType)
            );
            SortOptions byHotScore = SortOptions.of(t -> t
                    .field(fieldSort)
            );
            sortOptions.add(byHotScore);
        }
//
//        // 4.分页，
//        builder.from(param.getPageNum() == null ? 0 : (param.getPageNum() - 1) * ProductSearchConfig.PAGE_SIZE);

//        // 5.高亮，查询关键字不为空才有结果高亮
        Highlight highlight;
        if (!StringUtils.isEmpty(param.getKeyword())) {
            HighlightField highlightField = HighlightField.of(h -> h
                    .preTags("<b style='color:red'>")
                    .postTags("</b>")
            );
            highlight = Highlight.of(h -> h
                    .fields("skuTitle", highlightField)
            );
        } else {
            highlight = null;
        }
        //聚合分类
        // 6.聚合分析，分析得到的商品所涉及到的分类、品牌、规格参数，
        // term值的是分布情况，就是存在哪些值，每种值下有几个数据; size是取所有结果的前几种，(按id聚合后肯定是同一种，所以可以指定为1)
        // 6.1 分类部分，按照分类id聚合，划分出分类后，每个分类内按照分类名字聚合就得到分类名，不用再根据id再去查询数据库
        //新版不需要subAggregation了，可以直接aggregations嵌套，还可以把子聚合提出成一个对象使用
        Aggregation catelogNameAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("catelogName")
                        .size(1)
                )
        );
        Aggregation catelogAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("catelogId")
                ).aggregations("catelogNameAgg", catelogNameAgg)
        );
        // 6.2 品牌部分，按照品牌id聚合，划分出品牌后，每个品牌内按照品牌名字聚合就得到品牌名，不用再根据id再去查询数据库
        // 每个品牌内按照品牌logo聚合就得到品牌logo，不用再根据id再去查询数据库
        Aggregation brandNameAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("brandName")
                )
        );
        Aggregation brandImgAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("brandImg")
                )
        );
        Aggregation brandAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("brandId")
                ).aggregations("brandNameAgg", brandNameAgg)
                .aggregations("brandImgAgg", brandImgAgg)
        );

//        // 6.3 规格参数部分，按照规格参数id聚合，划分出规格参数后，每个品牌内按照规格参数名字聚合就得到规格参数名，不用再根据id再去查询数据库
//        // 每个规格参数内按照规格参数值聚合就得到规格参数值，不用再根据id再去查询数据库

        Aggregation attrNameAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("attrs.attrName")
                )
        );
        Aggregation attrValueAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("attrs.attrValue")
                )
        );
        Aggregation attrIdAgg = Aggregation.of(a -> a
                .terms(t -> t
                        .field("attrs.attrId")
                ).aggregations("attrNameAgg", attrNameAgg)
                .aggregations("attrValueAgg", attrValueAgg)
        );
        Aggregation attrAgg = Aggregation.of(a -> a
                .nested(t -> t
                        .path("attrs")
                ).aggregations("attrIdAgg", attrIdAgg)
        );

        // 2.5 规格属性
        // attrs=1_钢精:铝合&attrs=2_anzhuo:apple&attrs=3_lisi ==> attrs=[1_钢精:铝合,2_anzhuo:apple,3_lisi]
        List<String> attrs = param.getAttrs();
        if (!CollectionUtils.isEmpty(attrs)) {
            // 每个属性参数 attrs=1_钢精:铝合 ==》 nestedQueryFilter
            /**
             *          {
             *           "nested": {
             *             "path": "",
             *             "query": {
             *               "bool": {
             *                 "must": [
             *                   {},
             *                   {}
             *                 ]
             *               }
             *             }
             *           }
             *         },
             */
            for (String attr : attrs) {
                String[] attrInfo = attr.split("_");
                Query byAttrId = TermQuery.of(m -> m
                        .field("attrs.attrId")
                        .value(attrInfo[0])
                )._toQuery();
//                List<FieldValue> fieldValueList=new ArrayList<>();
//                for (String s : attrInfo) {
//                    fieldValueList.add(FieldValue.of(s));
//                }
                List<FieldValue> fieldValueList = Stream.of(attr.split("_"))
                        .map(FieldValue::of)
                        .collect(Collectors.toList());
                Query byAttrValue = TermsQuery.of(m -> m
                        .field("attrs.attrValue")
                        .terms(t->t
                                .value(fieldValueList)
                        )
                )._toQuery();
                Query bool = BoolQuery.of(b -> b
                        .must(byAttrId)
                        .must(byAttrValue))._toQuery();
                Query nestedQuery = NestedQuery.of(n -> n
                        .path("attrs")
                        .query(bool)
                        .scoreMode(ChildScoreMode.None)
                )._toQuery();
                filterQueries.add(nestedQuery);
            }
        }

// 搜索和过滤查询组合
        SearchResponse<SkuESModel> response = esClient.search(s -> s
                        .index(SearchConstant.ESIndex.ES_PRODUCT_INDEX)
                        .query(q -> q
                                .bool(b -> b
                                        .must(mustQueries)
                                        .filter(filterQueries)
                                )
                        ).sort(sortOptions)
                        .from(param.getPageNum() == null ? 0 : (param.getPageNum() - 1) * ProductSearchConfig.PAGE_SIZE)
                        .size(ProductSearchConfig.PAGE_SIZE)
                        .highlight(highlight)
//                        .aggregations("catelogAgg", catelogAgg)
                        .aggregations("catelogAgg", a -> a
                                .histogram(h -> h
                                        .field("catelogId")
                                        .interval(1.0)
                                ).aggregations("catelogNameAgg", b -> b
                                        .terms(h -> h
                                                .field("catelogName")
                                        )
                                )
                        ).aggregations("brandAgg", brandAgg)
                        .aggregations("attrAgg", attrAgg),
                SkuESModel.class
        );
        log.info(response.toString());
        return response;
    }


    /**
     * 老api rhlc从ES返回的结果构造出 指定的结构数据
     * @param response
     * @return
     */
//    private SearchResult buildSearchResult(SearchParam param, SearchResponse response) {
//
//        SearchResult result = new SearchResult();
//        SearchHits hits = response.getHits();
//        /**
//         * 全部商品数据
//         */
//        List<SkuESModel> esModels = Arrays.stream(hits.getHits()).map(hit -> {
//            // 每个命中的记录的_source部分是真正的数据的json字符串
//            String sourceAsString = hit.getSourceAsString();
//            SkuESModel esModel = JSON.parseObject(sourceAsString, SkuESModel.class);
//            if (!StringUtils.isEmpty(param.getKeyword())) {
//                String skuTitle = hit.getHighlightFields().get("skuTitle").getFragments()[0].toString();
//                esModel.setSkuTitle(skuTitle);
//            }
//            return esModel;
//        }).collect(Collectors.toList());
//        result.setSkuList(esModels);
//        /**
//         * 聚合结果--分类
//         */
//        Aggregations aggregations = response.getAggregations();
//        // debug模式下确定这个返回的具体类型
//        ParsedLongTerms catelogAgg = aggregations.get("catelogAgg");
//        // 每一个bucket是一种分类，有几个bucket就会有几个分类
//        List<SearchResult.CatelogVO> catelogs = catelogAgg.getBuckets().stream().map(bucket -> {
//            // debug查看下结果
//            long catelogId = bucket.getKeyAsNumber().longValue();
//            // debug模式下确定这个返回的具体类型
//            ParsedStringTerms catelogNameAgg = bucket.getAggregations().get("catelogNameAgg");
//            // 根据id分类后肯定是同一类，只可能有一种名字，所以直接取第一个bucket
//            String catelogName = catelogNameAgg.getBuckets().get(0).getKeyAsString();
//            SearchResult.CatelogVO catelogVO = new SearchResult.CatelogVO();
//            catelogVO.setCatelogId(catelogId);
//            catelogVO.setCatelogName(catelogName);
//            return catelogVO;
//        }).collect(Collectors.toList());
//        result.setCatelogs(catelogs);
//        /**
//         * 聚合结果--品牌，与上面过程类似
//         */
//        ParsedLongTerms brandAgg = aggregations.get("brandAgg");
//        List<SearchResult.BrandVO> brands = brandAgg.getBuckets().stream().map(bucket -> {
//            long brandId = bucket.getKeyAsNumber().longValue();
//            ParsedStringTerms brandNameAgg = bucket.getAggregations().get("brandNameAgg");
//            String brandName = brandNameAgg.getBuckets().get(0).getKeyAsString();
//            ParsedStringTerms brandImgAgg = bucket.getAggregations().get("brandImgAgg");
//            String brandImg = brandImgAgg.getBuckets().get(0).getKeyAsString();
//            SearchResult.BrandVO brandVO = new SearchResult.BrandVO();
//            brandVO.setBrandId(brandId);
//            brandVO.setBrandName(brandName);
//            brandVO.setBrandImg(brandImg);
//            return brandVO;
//        }).collect(Collectors.toList());
//        result.setBrands(brands);
//        /**
//         * 聚合结果--规格参数
//         */
//        ParsedNested attrAgg = aggregations.get("attrAgg");
//        ParsedLongTerms attrIdAgg = attrAgg.getAggregations().get("attrIdAgg");
//        List<SearchResult.AttrVO> attrs = attrIdAgg.getBuckets().stream().map(bucket -> {
//            long attrId = bucket.getKeyAsNumber().longValue();
//            ParsedStringTerms attrNameAgg = bucket.getAggregations().get("attrNameAgg");
//            // 根据id分类后肯定是同一类，只可能有一种名字，所以直接取第一个bucket
//            String attrName = attrNameAgg.getBuckets().get(0).getKeyAsString();
//            // 根据id分类后肯定是同一类，但是可以有多个值，所以会有多个bucket，把所有值组合起来
//            ParsedStringTerms attrValueAgg = bucket.getAggregations().get("attrValueAgg");
//            List<String> attrValue = attrValueAgg.getBuckets().stream().map(b -> b.getKeyAsString()).collect(Collectors.toList());
//            SearchResult.AttrVO attrVO = new SearchResult.AttrVO();
//            attrVO.setAttrId(attrId);
//            attrVO.setAttrName(attrName);
//            attrVO.setAttrValue(attrValue);
//            return attrVO;
//        }).collect(Collectors.toList());
//        result.setAttrs(attrs);
//        /**
//         * 分页信息
//         */
//        // 总记录数
//        result.setTotalCount(hits.getTotalHits().value);
//        // 每页大小
//        result.setPageSize(ProductSearchConfig.PAGE_SIZE);
//        // 总页数
//        result.setTotalPage((result.getTotalCount() + ProductSearchConfig.PAGE_SIZE - 1) / ProductSearchConfig.PAGE_SIZE);
//        // 当前页码
//        int pageNum = param.getPageNum() == null ? 1 : param.getPageNum();
//        result.setCurrPage(pageNum);
//        // 构建页码导航,以当前页为中心，连续5页
//        ArrayList<Integer> pageNavs = new ArrayList<>();
//        for (int i = pageNum - 2; i <= pageNum + 2; ++i) {
//            if (i <= 0) {
//                continue;
//            }
//            if (i >= result.getTotalPage()) {
//                break;
//            }
//            pageNavs.add(i);
//        }
//        result.setPageNavs(pageNavs);
//
//        List<SearchResult.BreadCrumbsVO> breadCrumbsVOS = new LinkedList<>();
//        /**
//         * 构建面包屑导航--参数品牌部分
//         */
//        List<Long> ids = param.getBrandId();
//        if (!CollectionUtils.isEmpty(ids)) {
//            R res = productFeignService.getBatch(ids);
//            if (res.getCode() == 0) {
//                List<BrandTO> brandTOS = res.getData(new TypeReference<List<BrandTO>>() {});
//                brandTOS.forEach(brandTO -> {
//                    SearchResult.BreadCrumbsVO crumb = new SearchResult.BreadCrumbsVO();
//                    crumb.setAttrName("品牌");
//                    crumb.setAttrValue(brandTO.getName());
//                    // 请求参数中去掉当前属性之后的链接地址
//                    String link = param.getQueryString().replace("&brandId=" + brandTO.getBrandId(), "").replace("brandId=" + brandTO.getBrandId(), "");
//                    crumb.setLink("http://search.gulimall.com/list.html?" + link);
//                    breadCrumbsVOS.add(crumb);
//                });
//            } else {
//                log.warn("ESSearch调用gulimall-product/brand/info/batch失败");
//            }
//        }
//
//        /**
//         * 构建面包屑导航，三级分类部分
//         */
//
//        /**
//         * 构建面包屑导航--规格参数部分
//         * 从请求参数规格参数部分，
//         * 请求参数中有规格参数部分条件，才构建
//         // &attrs=1_陶瓷:铝合金&attrs=2_anzhuo:apple
//         */
//        List<String> queryAttrs = param.getAttrs();
//        if (!CollectionUtils.isEmpty(queryAttrs)) {
//            List<SearchResult.BreadCrumbsVO> crumbsVOS = queryAttrs.stream().map(attrStr -> {
//                // id_value
//                String[] attrInfo = attrStr.split("_");
//                SearchResult.BreadCrumbsVO breadCrumbsVO = new SearchResult.BreadCrumbsVO();
//                breadCrumbsVO.setAttrValue(attrInfo[1]);
//                // 请求参数中去掉当前属性之后的链接地址
//                String link = "";
//                try {
//                    // request对路径进行了编码。我们得先把自己的参数编码。才能在路径正正确匹配并替换
//                    String encode = URLEncoder.encode(attrStr, "utf-8");
//                    // java编码后，空格会被替换为 + ，而浏览器会编码为 %20；英文()会被编码成%28,%29，而浏览器不会编码英文()
//                    // 所以我们还得把+替换为浏览器的规则
//                    encode = encode.replace("+", "%20").replace("%28", "(").replace("%29", ")");
//                    // 去掉 &attrs=1_陶瓷
//                    URLDecoder.decode(param.getQueryString(), "iso-8859-1");
//                    link = param.getQueryString().replace("&attrs=" + encode, "").replace("attrs=" + encode, "");
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                }
//                breadCrumbsVO.setLink("http://search.gulimall.com/list.html?" + link);
//                // 保存请求参数中的attrId
//                result.getParamAttrIds().add(Long.parseLong(attrInfo[0]));
//                // 远程调用
//                try {
//                    R r = productFeignService.info(Long.valueOf(attrInfo[0]));
//                    if (r.getCode() == 0) {
//                        AttrRespVO attrRespVO = r.getData("attr", AttrRespVO.class);
//                        breadCrumbsVO.setAttrName(attrRespVO.getAttrName());
//                    }
//                } catch (Exception e) {
//                    log.error("gulimall-search调用gulimall-product根据attrId查询attrInfo失败：{}", e);
//                }
//                return breadCrumbsVO;
//            }).collect(Collectors.toList());
//            breadCrumbsVOS.addAll(crumbsVOS);
//
//        }
//        // 保存所有面包屑
//        result.setBreadCrumbsNavs(breadCrumbsVOS);
//        // 返回结果
//        return result;
//    }
}
