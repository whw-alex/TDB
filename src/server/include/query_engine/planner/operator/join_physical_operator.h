#pragma once

#include "physical_operator.h"
#include "include/query_engine/structor/tuple/join_tuple.h"
#include "include/query_engine/planner/node/join_logical_node.h"

// TODO [Lab3] join算子的头文件定义，根据需要添加对应的变量和方法
class JoinPhysicalOperator : public PhysicalOperator
{
public:
  JoinPhysicalOperator();
  ~JoinPhysicalOperator() override = default;

  PhysicalOperatorType type() const override
  {
    return PhysicalOperatorType::JOIN;
  }

  JoinPhysicalOperator(std::unique_ptr<Expression> condition) {
    // for (auto &child : join_logical_node->children()) {
    //   children_.emplace_back(std::move(child));
    // }
    LOG_INFO("JoinPhysicalOperator  constructor condition called");
    condition_ = std::move(condition);
    LOG_INFO("condition type: %d", condition_->type());
  }
  // JoinPhysicalOperator *copy()
  // {
  //   return new JoinPhysicalOperator();
  // }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;
  Tuple *current_tuple() override;

private:
  Trx *trx_ = nullptr;
  JoinedTuple joined_tuple_;  //! 当前关联的左右两个tuple
  std::unique_ptr<Expression> condition_;
  Tuple* left_tuple_;
  Tuple* right_tuple_;

};
