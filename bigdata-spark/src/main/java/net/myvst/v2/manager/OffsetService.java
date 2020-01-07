package net.myvst.v2.manager;

import net.myvst.v2.db.DBOperator;
import net.myvst.v2.db.DataMapping;
import org.apache.kafka.common.TopicPartition;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public interface OffsetService extends DataMapping {
    Map<TopicPartition, Long> loadOffset(DBOperator db, Collection<String> topics) throws SQLException;
}
