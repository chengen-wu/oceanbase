/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the
 * Mulan PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL_ENG
#include "ob_table_mutiscan.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"

using namespace oceanbase;
using namespace oceanbase::table;
using namespace oceanbase::observer;

ObTableMultiScanQueryResultIterator::ObTableMultiScanQueryResultIterator()
    : schema_getter_guard_(), schema_cache_guard_(), credential_(),
      sess_guard_(), result_(),res_arr_(), allocator_("MultiScanRes", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),limit_(-1), offset_(0),
      simple_table_schema_(nullptr) {}

ObTableMultiScanQueryResultIterator::~ObTableMultiScanQueryResultIterator(){
  row_iter_->~BaseMultiScanIterator();
  // allocator_.clear();
}

int ObTableMultiScanQueryResultIterator::init(ObString &json_string) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(
          ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
              tenant_id_, schema_getter_guard_))) {
    LOG_WARN("get schema_getter_guard_ failed");
  } else if (OB_FAIL(TABLEAPI_SESS_POOL_MGR->get_sess_info(credential_,
                                                           sess_guard_))) {
    LOG_WARN("get sess_guard_ failed", K(ret));
  } else if (OB_FAIL(
                 ObJsonParser::check_json_syntax(json_string, &allocator_))) {
    LOG_WARN("json syntax invalid", K(ret));
  } else if (OB_FAIL(parse_table_name(json_string))) {
    LOG_WARN("parse table name failed", K(ret));
  } else if (OB_FAIL(parse_limit(json_string))) {
    LOG_WARN("parse limit failed", K(ret));
  } else if (OB_FAIL(parse_query(json_string,row_iter_))) {
    LOG_WARN("parse query failed", K(ret));
  }

  return ret;
}

int ObTableMultiScanQueryResultIterator::parse_table_name(
    ObString &json_string) {
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  ObJsonInType j_in_type = ObJsonInType::JSON_TREE;
  // parse table name
  ObString j_table_path_str("$.table_name");
  ObJsonPath j_table_path(j_table_path_str, &allocator_);
  j_table_path.parse_path();
  ObJsonSeekResult hit;
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator_, json_string,
                                               j_in_type, j_in_type, j_base))) {
    LOG_WARN("json string is valid", K(ret));
  } else if (OB_FAIL(j_base->seek(j_table_path, j_table_path.path_node_cnt(),
                                  true, false, hit))) {
    LOG_WARN("json seek failed", K(ret));
  } else {
    ObString table_name(hit[0]->get_data());
    if (OB_FAIL(init_schema_info(table_name))) {
      LOG_WARN("init schema info failed", K(ret));
    }
  }

  return ret;
}

int ObTableMultiScanQueryResultIterator::init_schema_info(
    ObString &table_name) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(schema_getter_guard_.get_simple_table_schema(
          tenant_id_, database_id_, table_name, false, /* is_index */
          simple_table_schema_))) {
    LOG_WARN("fail to get table schema", K(ret));
  }
  schema_cache_guard_.init(tenant_id_, simple_table_schema_->get_table_id(),
                           simple_table_schema_->get_schema_version(),
                           schema_getter_guard_);
  return ret;
}

int ObTableMultiScanQueryResultIterator::parse_query(ObString &json_string,BaseMultiScanIterator *&iter) {
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  ObArray<BaseMultiScanIterator *> iter_arr;
  ObJsonInType j_in_type = ObJsonInType::JSON_TREE;
  ObJsonBaseFactory::get_json_base(&allocator_, json_string, j_in_type,
                                   j_in_type, j_base);
  // parse table name
  ObString j_relation_path_str("$.relation");
  ObJsonPath j_relation_path(j_relation_path_str, &allocator_);
  j_relation_path.parse_path();
  ObJsonSeekResult hit;
  if (OB_FAIL(j_base->seek(j_relation_path, j_relation_path.path_node_cnt(),
                           true, false, hit))) {
    LOG_WARN("json seek failed", K(ret));
  }
  ObString relation(hit[0]->get_data());
  if (relation.compare("OR") == 0 || relation.compare("or") == 0 ||
      relation.compare("Or") == 0) {
    // or
    OrMultiScanIterator *or_iter = OB_NEWx(OrMultiScanIterator, &allocator_);
    if (OB_FAIL(parse_condition(json_string, or_iter->iter_arr_))) {
      LOG_WARN("parse condition failed", K(ret));
    } else if (OB_FAIL(or_iter->before_process())) {
      LOG_WARN("iter before process failed", K(ret));
    }
    iter = or_iter;

  } else if (relation.compare("AND") == 0 || relation.compare("and") == 0 ||
             relation.compare("And") == 0) {
    AndMultiScanIterator *and_iter = OB_NEWx(AndMultiScanIterator, &allocator_);
    if (OB_FAIL(parse_condition(json_string, and_iter->iter_arr_))) {
      LOG_WARN("parse condition failed", K(ret));
    } else if (OB_FAIL(and_iter->before_process())) {
      LOG_WARN("iter before process failed", K(ret));
    }
    iter = and_iter;
  } else {
    // todo
    // single
  }
  return ret;
}
int ObTableMultiScanQueryResultIterator::parse_limit(ObString &json_string) {
  int ret = OB_SUCCESS;
  ObIJsonBase *j_base = NULL;
  ObJsonInType j_in_type = ObJsonInType::JSON_TREE;
  ObIJsonBase *value = NULL;
  ObString limit_str("limit");
  if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator_, json_string,
                                               j_in_type, j_in_type, j_base))) {
    LOG_WARN("parse json failed", K(ret));
  } else if (OB_FAIL(j_base->object_iterator().get_value(limit_str, value))) {
    if(ret==OB_SEARCH_NOT_FOUND){
      ret=OB_SUCCESS;
      LOG_WARN("param has no limit");
    }
    LOG_WARN("get limit failed", K(ret));
  } else if (value->json_type() == common::ObJsonNodeType::J_INT) {
    limit_ = value->get_int();
  }
  return ret;
}
int ObTableMultiScanQueryResultIterator::parse_condition(
    ObString &json_string, ObArray<BaseMultiScanIterator *> &iter_arr) {
  int ret = OB_SUCCESS;
  BaseMultiScanIterator *iter = nullptr;
  iter_arr.reset();
  ObIJsonBase *j_base = NULL;
  ObJsonInType j_in_type = ObJsonInType::JSON_TREE;
  ObJsonBaseFactory::get_json_base(&allocator_, json_string, j_in_type,
                                   j_in_type, j_base);
  // parse condition
  ObString j_condition_path_str("$.condition[*]");
  ObJsonPath j_condition_path(j_condition_path_str, &allocator_);
  j_condition_path.parse_path();
  ObJsonSeekResult hit;
  if (OB_FAIL(j_base->seek(j_condition_path, j_condition_path.path_node_cnt(),
                           true, false, hit))) {
    LOG_WARN("json seek failed", K(ret));
  }
  for (int i = 0; i < hit.size(); i++) {
    if (OB_FAIL(get_table_iter(hit[i], iter))) {
      LOG_WARN("get multi scan iter failed", K(ret));
    } else if (iter_arr.push_back(iter)) {
      LOG_WARN("iter arr push back failed", K(ret));
    }
  }
  return ret;
}

int ObTableMultiScanQueryResultIterator::get_table_iter(
    ObIJsonBase *j_base, BaseMultiScanIterator *&iter) {
  int ret = OB_SUCCESS;
  ObString relation_str("relation");
  ObString key_str;
  j_base->object_iterator().get_key(key_str);
  // and or
  if (relation_str.compare(key_str)==0) {
    ObString query_json_str;
    ObJsonBuffer jbuf(&allocator_);
    j_base->print(jbuf,true);
    query_json_str=jbuf.string();
    if(OB_FAIL(parse_query(query_json_str,iter))){
      LOG_WARN("failed to parse query",K(ret));
    }
  }
  // single
  // col name
  else {
    ObString col_key("Q");
    ObIJsonBase *value = NULL;
    j_base->object_iterator().get_value(col_key, value);
    ObString col_name(value->get_data());
    // range
    ObNewRange *range;
    SingleMultiScanIterator *single_iter = nullptr;
    ObString range_key("range");
    value->reset();
    j_base->object_iterator().get_value(range_key, value);
    build_range(col_name, value, range);
    single_iter = OB_NEWx(SingleMultiScanIterator,&allocator_);
    single_iter->simple_table_schema_ = simple_table_schema_;
    single_iter->schema_guard_ = &schema_getter_guard_;
    single_iter->schema_cache_guard_ = &schema_cache_guard_;
    single_iter->simple_table_schema_ = simple_table_schema_;
    single_iter->credential_ = &credential_;
    single_iter->sess_guard_ = &sess_guard_;
    single_iter->set_query(range);
    if (OB_FAIL(single_iter->init())) {
      LOG_WARN("SingleMultiScanIterator init fail", K(ret));
    } else {
      iter = single_iter;
    }
  }
  return ret;
}

int ObTableMultiScanQueryResultIterator::build_range(const ObString &Q,
                                                     ObIJsonBase *range_j_base,
                                                     ObNewRange *&range) {
  int ret = OB_SUCCESS;
  bool inclusive_start = false;
  bool inclusive_end = false;
  common::ObRowkey start_key;
  common::ObRowkey end_key;
  ObObj *start_obj_ptr = nullptr;
  ObObj *end_obj_ptr = nullptr;
  ObIJsonBase *value = nullptr;
  ObString n_inf_str("-INF");
  ObString p_inf_str("INF");
  ObString tmp_str;
  // inclusive_start
  range_j_base->get_array_element(2, value);
  if (value->json_type() == common::ObJsonNodeType::J_BOOLEAN) {
    inclusive_start = value->get_boolean();
  } else {
    LOG_WARN("json type is not a boolean");
  }
  // inclusive_end
  value->reset();
  range_j_base->get_array_element(3, value);

  if (value->json_type() == common::ObJsonNodeType::J_BOOLEAN) {
    inclusive_end = value->get_boolean();
  } else {
    LOG_WARN("json type is not a boolean");
  }
  // start
  value->reset();
  int32_t start_int = INT32_MIN;
  range_j_base->get_array_element(0, value);
  if (value->json_type() == common::ObJsonNodeType::J_INT) {
    start_int = value->get_int();
  } else if (value->json_type() == common::ObJsonNodeType::J_STRING &&
             n_inf_str.compare(value->get_data()) == 0) {
    start_int = INT32_MIN;
  }
  if (OB_ISNULL(start_obj_ptr = static_cast<ObObj *>(
                    allocator_.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    start_obj_ptr[0].set_varbinary(Q);
    start_obj_ptr[1].set_int32(start_int);
    start_key.assign(start_obj_ptr, 2);
  }
  // end
  value->reset();
  int32_t end_int = INT32_MAX;
  range_j_base->get_array_element(1, value);
  if (value->json_type() == common::ObJsonNodeType::J_INT) {
    end_int = value->get_int();
  } else if (value->json_type() == common::ObJsonNodeType::J_STRING &&
             p_inf_str.compare(value->get_data()) == 0) {
    end_int = INT32_MAX;
  }
  if (OB_ISNULL(end_obj_ptr = static_cast<ObObj *>(
                    allocator_.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for ObObj", K(ret));
  } else {
    end_obj_ptr[0].set_varbinary(Q);
    end_obj_ptr[1].set_int32(end_int);
    end_key.assign(end_obj_ptr, 2);
  }
  // build range
  range = nullptr;
  if (OB_ISNULL(range = OB_NEWx(ObNewRange, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObNewRange", K(ret));
  } else {
    range->start_key_ = start_key;
    range->end_key_ = end_key;
    if (inclusive_start) {
      range->border_flag_.set_inclusive_start();
    }
    if (inclusive_end) {
      range->border_flag_.set_inclusive_end();
    }
    range->table_id_ = simple_table_schema_->get_table_id();
  }
  return ret;
}
int ObTableMultiScanQueryResultIterator::query_and_result() {
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;
  if (NULL == row_iter_) {
    ret = OB_ERR_UNEXPECTED;
  }
  ObString property_name("K");
  result_.add_property_name(property_name);
  while (OB_SUCC(ret) && OB_SUCC(row_iter_->get_next_row(row))) {
    if (limit_-- == 0) {
      ret = OB_ITER_END;
      break;
    }
    ObFastFormatInt ffi(row->get_cell(0).get_int());
    ObString int_str(ffi.length(), ffi.ptr());
    ObString res_str;
    ob_write_string(allocator_,int_str,res_str);
    res_arr_.push_back(res_str);
    result_.add_row(*row);
  }
  LOG_WARN("MultiScan debug", K(ret), K(result_));

  return ret;
}

ObNewRow *BaseMultiScanIterator::get_last_row() { return last_row_; }
int BaseMultiScanIterator::get_last_row(ObNewRow *&row) {
  int ret = OB_SUCCESS;
  row = last_row_;
  return ret;
}
SingleMultiScanIterator::SingleMultiScanIterator()
    : tb_ctx_(allocator_), query_(), row_iter_(), trans_param_() {}

int SingleMultiScanIterator::set_query(ObNewRange *range) {
  int ret = OB_SUCCESS;
  ObString index_name("idx_QV");
  ObString columns_str("K");
  query_.add_scan_range(*range);
  query_.set_scan_index(index_name);
  query_.add_select_column(columns_str);
  return ret;
}

int SingleMultiScanIterator::init() {
  int ret = OB_SUCCESS;
  ObTableApiSpec *spec = nullptr;
  ObTableApiExecutor *executor = nullptr;
  observer::ObReqTimeGuard req_timeinfo_guard;
  ObTableApiCacheGuard cache_guard;
  if (OB_FAIL(init_tb_ctx(cache_guard))) {
    LOG_WARN("fail to init table ctx", K(ret));
  } else if (OB_FAIL(
                 cache_guard.get_spec<TABLE_API_EXEC_SCAN>(&tb_ctx_, spec))) {
    LOG_WARN("fail to get spec from cache", K(ret));
  } else if (OB_FAIL(spec->create_executor(tb_ctx_, executor))) {
    LOG_WARN("fail to generate executor", K(ret), K(tb_ctx_));
  } else if (OB_FAIL(start_trans())) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(row_iter_.open(
                 static_cast<ObTableApiScanExecutor *>(executor)))) {
    LOG_WARN("fail to open scan row iterator", K(ret));
  } else if (OB_FAIL(init_is_ordered())) {
    LOG_WARN("fail to init is_ordered_", K(ret));
  }
  return ret;
}
int SingleMultiScanIterator::init_tb_ctx(ObTableApiCacheGuard &cache_guard) {
  int ret = OB_SUCCESS;
  ObExprFrameInfo *expr_frame_info = nullptr;
  bool is_weak_read = false;
  tb_ctx_.set_scan(true);
  tb_ctx_.set_entity_type(ObTableEntityType::ET_DYNAMIC);
  tb_ctx_.set_schema_cache_guard(schema_cache_guard_);
  tb_ctx_.set_schema_guard(schema_guard_);
  tb_ctx_.set_simple_table_schema(simple_table_schema_);
  tb_ctx_.set_sess_guard(sess_guard_);

  const int64_t ONE_TASK_TIMEOUT = 1 * 60 * 1000 * 1000l; // 1min
  if (tb_ctx_.is_init()) {
    LOG_INFO("tb ctx has been inited", K(tb_ctx_));
  } else if (OB_FAIL(tb_ctx_.init_common(
                 *credential_, simple_table_schema_->get_tablet_id(),
                 ONE_TASK_TIMEOUT + common::ObTimeUtility::current_time()))) {
    LOG_WARN("fail to init table ctx common part", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_scan(query_, is_weak_read,
                                       simple_table_schema_->get_table_id()))) {
    LOG_WARN("fail to init table ctx scan part", K(ret));
  } else if (OB_FAIL(cache_guard.init(&tb_ctx_))) {
    LOG_WARN("fail to init cache guard", K(ret));
  } else if (OB_FAIL(cache_guard.get_expr_info(&tb_ctx_, expr_frame_info))) {
    LOG_WARN("fail to get expr frame info from cache", K(ret));
  } else if (OB_FAIL(ObTableExprCgService::alloc_exprs_memory(
                 tb_ctx_, *expr_frame_info))) {
    LOG_WARN("fail to alloc expr memory", K(ret));
  } else if (OB_FAIL(tb_ctx_.init_exec_ctx())) {
    LOG_WARN("fail to init exec ctx", K(ret), K(tb_ctx_));
  } else {
    tb_ctx_.set_init_flag(true);
    tb_ctx_.set_expr_info(expr_frame_info);
  }
  return ret;
}

int SingleMultiScanIterator::start_trans() {
  int ret = OB_SUCCESS;
  bool is_readonly = true;
  ObTableConsistencyLevel consistency_level = ObTableConsistencyLevel::STRONG;
  if (OB_FAIL(trans_param_.init(is_readonly, consistency_level,
                                tb_ctx_.get_ls_id(), tb_ctx_.get_timeout_ts(),
                                tb_ctx_.need_dist_das()))) {
    LOG_WARN("fail to init trans param", K(ret));
  } else if (OB_FAIL(ObTableTransUtils::start_trans(trans_param_))) {
    LOG_WARN("fail to start trans", K(ret), K(trans_param_));
  } else if (OB_FAIL(tb_ctx_.init_trans(trans_param_.trans_desc_,
                                        trans_param_.tx_snapshot_))) {
    LOG_WARN("fail to init tb_ctx trans", K(ret));
  }
  return ret;
}
int SingleMultiScanIterator::init_is_ordered() {
  int ret = OB_SUCCESS;
  if (query_.get_range_count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("range size < 0", K(ret));
  } else if (OB_SUCC(ret)) {
    ObNewRange range = query_.get_scan_ranges().at(0);
    int compare_ret = range.start_key_.compare(range.end_key_);
    is_ordered_ = compare_ret == 0;
  }
  return ret;
}
int SingleMultiScanIterator::get_next_row(ObNewRow *&row) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(row_iter_.get_next_row(row))) {
    LOG_WARN("fail to get next row", K(ret));
    last_row_ = NULL;
  } else {
    last_row_ = row;
  }
  return ret;
}

OrMultiScanIterator::OrMultiScanIterator() : iter_arr_(), rowkey_set_() {
  
}
int OrMultiScanIterator::before_process() {
  int ret = OB_SUCCESS;
  // init is_ordered_
  is_ordered_ = true;
  for (int i = 0; i < iter_arr_.count() && is_ordered_; ++i) {
    is_ordered_ = is_ordered_ && iter_arr_[i]->is_ordered_;
  }
  // build iter tree
  if (!is_ordered_) {
    rowkey_set_.create(400003, "RowkeySet", "HashNode");
    ObArray<int32_t> ordered_idx_list;
    for (int i = 0; i < iter_arr_.count(); ++i) {
      if (iter_arr_[i]->is_ordered_) {
        ordered_idx_list.push_back(i);
      }
    }
    if (ordered_idx_list.count() >= 2) {
      OrMultiScanIterator *ordered_or_iter =
          OB_NEWx(OrMultiScanIterator, &allocator_);
      for (int i = 0; i < ordered_idx_list.count(); i++) {
        int index = ordered_idx_list[i] - i;
        ordered_or_iter->iter_arr_.push_back(iter_arr_[index]);
        iter_arr_.remove(index);
      }
      ordered_or_iter->before_process();
      iter_arr_.push_back(ordered_or_iter);
    }
  }
  return ret;
}
int OrMultiScanIterator::get_next_row(ObNewRow *&row) {
  int ret = OB_SUCCESS;
  if (iter_arr_.count() == 0) {
    last_row_ = nullptr;
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret) && is_ordered_) { // ordered union
    ObNewRow *current_row = nullptr;
    BaseMultiScanIterator *iter = nullptr;
    ObNewRow *new_row = OB_NEWx(ObNewRow, &allocator_);
    for (int i = 0; i < iter_arr_.count(); i++) { // get min row
      iter = iter_arr_[i];
      ObNewRow *last_row = nullptr;
      if (OB_FAIL(iter->get_last_row(last_row))) {
        LOG_WARN("fail to get last row", K(ret));
      }
      if (NULL == last_row) {                        // first OR end
        if (OB_SUCC(iter->get_next_row(last_row))) { // first
          // do nothing
        } else { // end
          iter->~BaseMultiScanIterator();
          iter_arr_.remove(i);
          i--;
          continue;
        }
      }
      if (NULL == current_row) {
        current_row = last_row;
      }
      if (compare_row(current_row, last_row) > 0) {
        current_row = last_row;
        continue;
      }
    }
    if (OB_ISNULL(current_row)) {
      last_row_ = nullptr;
      ret = OB_ITER_END;
    } else if (OB_FAIL(
                   common::ob_write_row(allocator_, *current_row, *new_row))) {
      LOG_WARN("failed to copy row", K(ret));
    } else {
      last_row_ = new_row;
      row = new_row;
      for (int i = 0; i < iter_arr_.count(); i++) {
        iter = iter_arr_[i];
        ObNewRow *last_row = nullptr;
        if (OB_FAIL(iter->get_last_row(last_row))) {
          LOG_WARN("fail to get last row", K(ret));
        } else if (compare_row(row, last_row) == 0) {
          iter->get_next_row(last_row);
        }
      }
    }
  } else if (OB_SUCC(ret) && !is_ordered_) { // unordered union

    ObNewRow *current_row = nullptr;
    BaseMultiScanIterator *iter = nullptr;
    ObNewRow *new_row = nullptr;
    while (NULL == current_row && OB_SUCC(ret)) {
      if (iter_arr_.count() == 0) {
        ret = OB_ITER_END;
        break;
      }
      iter = iter_arr_[0];
      ret = iter->get_next_row(iter->last_row_);
      if (ret == OB_ITER_END) {
        iter->~BaseMultiScanIterator();
        iter_arr_.remove(0);
        ret = OB_SUCCESS;
        continue;
      } else if (OB_SUCC(ret)) {
        int64_t rowkey = iter->get_last_row()->get_cell(0).get_int();
        int hash_ret = rowkey_set_.exist_refactored(rowkey);
        if (hash_ret == OB_HASH_EXIST) {
          continue;
        } else if (hash_ret == OB_HASH_NOT_EXIST) {
          rowkey_set_.set_refactored(rowkey);
        }
        current_row = iter->get_last_row();
        new_row = OB_NEWx(ObNewRow, &allocator_);
        common::ob_write_row(allocator_, *current_row, *new_row);
        row = new_row;
        last_row_ = new_row;
        break;
      }
    }
  }
  return ret;
}

/***
when left < right return negative
when left == right return 0
when left > right return postive
*/
int64_t OrMultiScanIterator::compare_row(ObNewRow *left, ObNewRow *right) {
  return left->get_cell(0).get_int() - right->get_cell(0).get_int();
}

AndMultiScanIterator::AndMultiScanIterator()
    : iter_arr_(), left_rowkey_set_(), right_rowkey_set_(),
      left_iter_end_(false), right_iter_end_(false) {}

int AndMultiScanIterator::before_process() {
  int ret = OB_SUCCESS;
  // init is_ordered_
  is_ordered_ = true;
  for (int i = 0; i < iter_arr_.count() && is_ordered_; ++i) {
    is_ordered_ = is_ordered_ && iter_arr_[i]->is_ordered_;
  }
  // build iter tree
  if (is_ordered_) {
    // do nothing
  } else {
    ObArray<int32_t> ordered_idx_list;
    for (int i = 0; i < iter_arr_.count(); ++i) {
      if (iter_arr_[i]->is_ordered_) {
        ordered_idx_list.push_back(i);
      }
    }
    if (ordered_idx_list.count() >= 2) {
      AndMultiScanIterator *and_dered_or_iter =
          OB_NEWx(AndMultiScanIterator, &allocator_);
      for (int i = 0; i < ordered_idx_list.count(); i++) {
        int index = ordered_idx_list[i] - i;
        and_dered_or_iter->iter_arr_.push_back(iter_arr_[index]);
        iter_arr_.remove(index);
      }
      and_dered_or_iter->before_process();
      iter_arr_.push_back(and_dered_or_iter);
    }
    // build tree
    if (iter_arr_.count() > 2) {
      AndMultiScanIterator *left_iter =
          OB_NEWx(AndMultiScanIterator, &allocator_);
      AndMultiScanIterator *right_iter =
          OB_NEWx(AndMultiScanIterator, &allocator_);
      int mid_idx = (iter_arr_.count() + 1) / 2;
      for (int i = 0; i < mid_idx; i++) {
        left_iter->iter_arr_.push_back(iter_arr_[i]);
      }
      for (int i = mid_idx; i < iter_arr_.count(); i++) {
        right_iter->iter_arr_.push_back(iter_arr_[i]);
      }
      left_iter->before_process();
      right_iter->before_process();
      iter_arr_.reset();
      iter_arr_.push_back(left_iter);
      iter_arr_.push_back(right_iter);
    }
    // init hash_set
    if (iter_arr_.count() == 2) {
      if (OB_FAIL(left_rowkey_set_.create(400003))) {
        LOG_WARN("hash set allocate memory failed");
      } else if (OB_FAIL(right_rowkey_set_.create(400003))) {
        LOG_WARN("hash set allocate memory failed");
      }
    }
  }
  return ret;
}

int AndMultiScanIterator::get_next_row(ObNewRow *&row) {
  // todo
  int ret = OB_SUCCESS;
  if (iter_arr_.count() == 0) {
    last_row_ = nullptr;
    ret = OB_ITER_END;
  }
  if (OB_SUCC(ret) && iter_arr_.count() == 1) {
    ret = iter_arr_[0]->get_next_row(row);
    last_row_ = row;
  } else if (OB_SUCC(ret) && is_ordered_) { // ordered intersect
    ObNewRow *current_row = nullptr;
    BaseMultiScanIterator *iter = nullptr;
    ObNewRow *new_row = nullptr;
    for (int i = 0; i < iter_arr_.count() && OB_SUCC(ret); i++) {
      iter = iter_arr_[i];
      ObNewRow *last_row = nullptr;
      if (OB_FAIL(iter->get_last_row(last_row))) {
        LOG_WARN("fail to get last row", K(ret));
      }
      if (NULL == last_row) {                        // first or end
        if (OB_SUCC(iter->get_next_row(last_row))) { // first iter
          // do nothing
        } else { // iter end
          last_row_ = nullptr;
          break;
        }
      }
      if (NULL == current_row) {
        current_row = last_row;
      }
      int64_t compare_ret = compare_row(current_row, last_row);
      if (compare_ret == 0) {
        continue;
      } else if (compare_ret > 0) { // current_row > last_row
        while (OB_SUCC(ret)) {
          if (OB_FAIL(iter->get_next_row(last_row))) {
            LOG_WARN("get next row end or error", K(ret));
            break;
          } else if (last_row == NULL) {
            ret = OB_ITER_END;
            break;
          }
          if (compare_row(current_row, last_row) > 0) {
            continue;
          } else {
            break;
          }
        }
        if (compare_row(current_row, last_row) == 0) {
          continue;
        } else { // current_row < last_row
          current_row = last_row;
          i = -1;
          continue;
        }
      } else { // current_row < last_row
        current_row = last_row;
        i = -1;
        continue;
      }
    } // end for
    if (OB_SUCC(ret)) {
      new_row = OB_NEWx(ObNewRow, &allocator_);
      common::ob_write_row(allocator_, *current_row, *new_row);
      last_row_ = new_row;
      row = new_row;
      iter_arr_[0]->get_next_row(iter_arr_[0]->last_row_);
    }
  } else if (OB_SUCC(ret) && !is_ordered_) { // unordered intersect
    // todo
    while (OB_SUCC(ret)) {
      ObNewRow *new_row = nullptr;
      BaseMultiScanIterator *iter = nullptr;
      common::hash::ObHashSet<int64_t> *other_set;
      common::hash::ObHashSet<int64_t> *current_set;
      bool *current_iter_end;
      int iter_ret = OB_SUCCESS;
      if ((left_iter_end_ && right_iter_end_) ||
          (left_iter_end_ && left_rowkey_set_.size() == 0) ||
          (right_iter_end_ && right_rowkey_set_.size() == 0)) {
        ret = OB_ITER_END;
        break;
      } else if ((left_rowkey_set_.size() < right_rowkey_set_.size() &&
                  !left_iter_end_) ||
                 (left_rowkey_set_.size() >= right_rowkey_set_.size() &&
                  right_iter_end_)) { // iter left
        iter = iter_arr_[0];
        other_set = &right_rowkey_set_;
        current_set = &left_rowkey_set_;
        current_iter_end = &left_iter_end_;
      } else if ((left_rowkey_set_.size() < right_rowkey_set_.size() &&
                  left_iter_end_) ||
                 (left_rowkey_set_.size() >= right_rowkey_set_.size() &&
                  !right_iter_end_)) { // iter right
        iter = iter_arr_[1];
        other_set = &left_rowkey_set_;
        current_set = &right_rowkey_set_;
        current_iter_end = &right_iter_end_;
      }
      iter_ret = iter->get_next_row(new_row);
      if (new_row == NULL || iter_ret == OB_ITER_END) {
        *current_iter_end = true;
        continue;
      } else {
        int64_t rowkey = new_row->get_cell(0).get_int();
        int hash_ret = other_set->exist_refactored(rowkey);
        if (hash_ret == OB_HASH_EXIST) {
          row = new_row;
          last_row_ = new_row;
          other_set->erase_refactored(rowkey);
          break;
        } else {
          current_set->set_refactored(rowkey);
          continue;
        }
      }
    } // end while
  }
  if (OB_FAIL(ret)) {
    row = nullptr;
    last_row_ = nullptr;
  }
  return ret;
}

int64_t AndMultiScanIterator::compare_row(ObNewRow *left, ObNewRow *right) {
  return left->get_cell(0).get_int() - right->get_cell(0).get_int();
}

BaseMultiScanIterator::~BaseMultiScanIterator(){
  allocator_.clear();
}
OrMultiScanIterator::~OrMultiScanIterator(){
  for(int i=0;i<iter_arr_.count();i++){
    iter_arr_[i]->~BaseMultiScanIterator();
  }
  rowkey_set_.destroy();
}

 AndMultiScanIterator::~AndMultiScanIterator(){
  for(int i=0;i<iter_arr_.count();i++){
    iter_arr_[i]->~BaseMultiScanIterator();
  }
  left_rowkey_set_.destroy();
  right_rowkey_set_.destroy();
 }