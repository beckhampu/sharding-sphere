/*
 * Copyright 2016-2018 shardingsphere.io.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package io.shardingsphere.core.parsing.antlr.filler.impl;

import io.shardingsphere.core.metadata.table.ShardingTableMetaData;
import io.shardingsphere.core.parsing.antlr.filler.SQLStatementFiller;
import io.shardingsphere.core.parsing.antlr.sql.segment.LimitSegment;
import io.shardingsphere.core.parsing.parser.context.limit.Limit;
import io.shardingsphere.core.parsing.parser.context.limit.LimitValue;
import io.shardingsphere.core.parsing.parser.sql.SQLStatement;
import io.shardingsphere.core.parsing.parser.sql.dql.select.SelectStatement;
import io.shardingsphere.core.parsing.parser.token.OffsetToken;
import io.shardingsphere.core.parsing.parser.token.RowCountToken;
import io.shardingsphere.core.rule.ShardingRule;

/**
 * Limit filler.
 *
 * @author duhongjun
 */
public final class LimitFiller implements SQLStatementFiller<LimitSegment> {
    
    @Override
    public void fill(final LimitSegment sqlSegment, final SQLStatement sqlStatement, final String sql, final ShardingRule shardingRule, final ShardingTableMetaData shardingTableMetaData) {
        SelectStatement selectStatement = (SelectStatement) sqlStatement;
        Limit limit = new Limit(sqlSegment.getDatabaseType());
        selectStatement.setLimit(limit);
        if (sqlSegment.getOffset().isPresent()) {
            limit.setOffset(new LimitValue(sqlSegment.getOffset().get().getValue(), sqlSegment.getOffset().get().getIndex(), false));
            if (-1 == sqlSegment.getOffset().get().getIndex()) {
                selectStatement.getSQLTokens().add(new OffsetToken(sqlSegment.getOffset().get().getBeginPosition(), sqlSegment.getOffset().get().getValue()));
            }
        }
        limit.setRowCount(new LimitValue(sqlSegment.getRowCount().getValue(), sqlSegment.getRowCount().getIndex(), false));
        if (-1 == sqlSegment.getRowCount().getIndex()) {
            selectStatement.getSQLTokens().add(new RowCountToken(sqlSegment.getRowCount().getBeginPosition(), sqlSegment.getRowCount().getValue()));
        }
    }
}
