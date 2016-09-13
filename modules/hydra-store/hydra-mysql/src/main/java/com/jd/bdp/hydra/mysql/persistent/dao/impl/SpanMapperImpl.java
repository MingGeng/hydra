

package com.jd.bdp.hydra.mysql.persistent.dao.impl;

import java.util.List;

import org.mybatis.spring.SqlSessionTemplate;

import com.jd.bdp.hydra.Span;
import com.jd.bdp.hydra.mysql.persistent.dao.SpanMapper;

/**
 * User: biandi
 * Date: 13-5-8
 * Time: 下午4:54
 */
public class SpanMapperImpl implements SpanMapper{

    private SqlSessionTemplate sqlSession;

    @Override
    public void addSpan(Span span) {
        sqlSession.insert("addSpan",span);
    }

    @Override
    public List<Span> findSpanByTraceId(Long traceId) {
        return (List<Span>) sqlSession.selectList("findSpanByTraceId", traceId);
    }

    @Override
    public void deleteAllSpans() {
        sqlSession.delete("deleteAllSpan");
    }

    public void setSqlSession(SqlSessionTemplate sqlSession) {
        this.sqlSession = sqlSession;
    }
}
