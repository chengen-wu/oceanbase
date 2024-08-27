/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_MUTISCAN_H
#define _OB_TABLE_MUTISCAN_H 1

#include "common/ob_range.h"
#include "ob_table_cache.h"
#include "ob_table_context.h"
#include "ob_table_executor.h"
#include "ob_table_scan_executor.h"
#include "ob_table_trans_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"

namespace oceanbase {

namespace table {
class BaseMultiScanIterator;
class ObTableMultiScanQueryResultIterator {
public:
  ObSchemaGetterGuard schema_getter_guard_;
  ObKvSchemaCacheGuard schema_cache_guard_;
  ObTableApiCredential credential_;
  ObTableApiSessGuard sess_guard_;
  BaseMultiScanIterator *row_iter_;
  ObTableQueryResult result_;
  ObArray<ObString> res_arr_;
  common::ObArenaAllocator allocator_;
  int limit_;
  int offset_;

  ObTableMultiScanQueryResultIterator();
  ~ObTableMultiScanQueryResultIterator();

  int init(ObString &json_string);

  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void set_database_id(uint64_t database_id) { database_id_ = database_id; }
  void set_credential_(ObTableApiCredential &credential) {
    credential_.user_id_ = credential.user_id_;
    credential_.tenant_id_ = credential.tenant_id_;
    credential_.cluster_id_ = credential.cluster_id_;
    credential_.database_id_ = credential.database_id_;
    credential_.expire_ts_ = credential.expire_ts_;
    credential_.hash_val_ = credential.hash_val_;
  }
  int query_and_result();

private:
  uint64_t tenant_id_;
  uint64_t database_id_;
  const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;

  int parse_table_name(ObString &json_string);
  int init_schema_info(ObString &table_name);
  int parse_query(ObString &json_string, BaseMultiScanIterator *&iter);
  int parse_query(ObIJsonBase *j_base, BaseMultiScanIterator *&iter);
    int parse_condition(ObString & json_string,
                        ObArray<BaseMultiScanIterator *> & condition_arr);

    int get_table_iter(ObIJsonBase * j_base, BaseMultiScanIterator * &iter);
    int build_range(const ObString &Q, ObIJsonBase *range_j_base,
                    ObNewRange *&range);
    int parse_limit(ObString & json_string);
  };

  class BaseMultiScanIterator {
  public:
    bool is_ordered_;
    ObNewRow *last_row_;
    common::ObArenaAllocator allocator_;

    BaseMultiScanIterator()
        : is_ordered_(false), last_row_(nullptr),
          allocator_("MultiScanItr", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()) {}
    virtual ~BaseMultiScanIterator();
    virtual int get_next_row(ObNewRow *&row) = 0;
    int to_string(const char *buffer, const int64_t length) const { return 0; }
    ObNewRow *get_last_row();
    int get_last_row(ObNewRow *&row);

  private:
  };

  class SingleMultiScanIterator : public BaseMultiScanIterator {
  public:
    SingleMultiScanIterator();
    virtual ~SingleMultiScanIterator() { allocator_.clear(); }

    const share::schema::ObSimpleTableSchemaV2 *simple_table_schema_;
    ObSchemaGetterGuard *schema_guard_;
    ObKvSchemaCacheGuard *schema_cache_guard_;
    ObTableApiCredential *credential_;
    ObTableApiSessGuard *sess_guard_;

    table::ObTableCtx tb_ctx_;
    table::ObTableQuery query_;
    ObTableApiScanRowIterator row_iter_;
    ObTableTransParam trans_param_;
    virtual int get_next_row(ObNewRow *&row);
    int set_query(ObNewRange *range);
    int init();
    int init_tb_ctx(ObTableApiCacheGuard &cache_guard);
    int start_trans();
    int init_is_ordered();
  };

  class OrMultiScanIterator : public BaseMultiScanIterator {
  public:
    ObArray<BaseMultiScanIterator *> iter_arr_;

    OrMultiScanIterator();
    virtual ~OrMultiScanIterator();
    virtual int get_next_row(ObNewRow *&row);
    int before_process();

    int64_t compare_row(ObNewRow *left, ObNewRow *right);

  private:
    common::hash::ObHashSet<int64_t> rowkey_set_;
  };

  class AndMultiScanIterator : public BaseMultiScanIterator {
  public:
    ObArray<BaseMultiScanIterator *> iter_arr_;
    AndMultiScanIterator();
    virtual ~AndMultiScanIterator();
    virtual int get_next_row(ObNewRow *&row);
    int before_process();
    int64_t compare_row(ObNewRow *left, ObNewRow *right);

  private:
    common::hash::ObHashSet<int64_t> left_rowkey_set_;
    common::hash::ObHashSet<int64_t> right_rowkey_set_;
    bool left_iter_end_;
    bool right_iter_end_;
  };

} // end namespace table

} // end namespace oceanbase

#endif /* _OB_TABLE_MUTISCAN_H */