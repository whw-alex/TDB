#include "include/query_engine/planner/operator/join_physical_operator.h"

/* TODO [Lab3] join的算子实现，需要根据join_condition实现Join的具体逻辑，
  最后将结果传递给JoinTuple, 并由current_tuple向上返回
 JoinOperator通常会遵循下面的被调用逻辑：
 operator.open()
 while(operator.next()){
    Tuple *tuple = operator.current_tuple();
 }
 operator.close()
*/

JoinPhysicalOperator::JoinPhysicalOperator() = default;

// 执行next()前的准备工作, trx是之后事务中会使用到的，这里不用考虑
RC JoinPhysicalOperator::open(Trx *trx)
{
  if (children_.empty())
  {
    return RC::SUCCESS;
  }
  PhysicalOperator *left_child = children_[0].get();
  RC rc = left_child->open(trx);
  if (rc != RC::SUCCESS)
  {
    LOG_WARN("failed to init child operator: %s", strrc(rc));
    return rc;
  }
  PhysicalOperator *right_child = children_[1].get();
  rc = right_child->open(trx);
  if (rc != RC::SUCCESS)
  {
    LOG_WARN("failed to init child operator: %s", strrc(rc));
    return rc;
  }
  trx_ = trx;


  return RC::SUCCESS;
}

// 计算出接下来需要输出的数据，并将结果set到join_tuple中
// 如果没有更多数据，返回RC::RECORD_EOF
RC JoinPhysicalOperator::next()
{
  LOG_INFO("JoinPhysicalOperator next called children size %d", children_.size());
  if (children_.empty())
  {
    return RC::RECORD_EOF;
  }
  LOG_INFO("children_[0] type %d", children_[0]->type());
  LOG_INFO("children_[1] type %d", children_[1]->type());
  if (left_tuple_ == nullptr)
  {
    children_[0]->next();
    left_tuple_ = children_[0]->current_tuple();
    LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());
  }
  else
  {
    LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());
  }
  bool find = false;
  while (!find)
  {
    while (!find && children_[1]->next() == RC::SUCCESS)
    {
      right_tuple_ = children_[1]->current_tuple();
      LOG_INFO("right_tuple_ to string %s", right_tuple_->to_string().c_str());
      JoinedTuple joined_tuple;
      joined_tuple.set_left(left_tuple_);
      joined_tuple.set_right(right_tuple_);
      Value result;
      RC rc = condition_->get_value(joined_tuple, result);
      if (rc != RC::SUCCESS)
      {
        return rc;
      }
      if (result.get_boolean())
      {
        joined_tuple_.set_left(left_tuple_);
        joined_tuple_.set_right(right_tuple_);
        find = true;
        right_tuple_ = nullptr;
        LOG_INFO("find the right tuple");
        return RC::SUCCESS;
      }
      right_tuple_ = nullptr;
    }
    if (children_[1]->next() == RC::RECORD_EOF)
    {
      LOG_INFO("right child eof");
      if (children_[0]->next() == RC::RECORD_EOF)
      {
        return RC::RECORD_EOF;
      }
      left_tuple_ = children_[0]->current_tuple();
      LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());
      children_[1]->close();
      children_[1]->open(trx_);
    }

  }
  return RC::RECORD_EOF;


  // while (!find)
  // {
  //   while (!find && children_[1]->next() == RC::SUCCESS)
  //   {
  //     right_tuple_ = children_[1]->current_tuple();
  //     LOG_INFO("right_tuple_ to string %s", right_tuple_->to_string().c_str());
  //     JoinedTuple joined_tuple;
  //     joined_tuple.set_left(left_tuple_);
  //     joined_tuple.set_right(right_tuple_);
  //     Value result;
  //     RC rc = condition_->get_value(joined_tuple, result);
  //     if (rc != RC::SUCCESS)
  //     {
  //       return rc;
  //     }
  //     if (result.get_boolean())
  //     {
  //       joined_tuple_.set_left(left_tuple_);
  //       joined_tuple_.set_right(right_tuple_);
  //       find = true;
  //       right_tuple_ = nullptr;
  //       LOG_INFO("find the right tuple");
  //       if (children_[1]->next() == RC::RECORD_EOF)
  //       {
  //         LOG_INFO("right child eof");
  //         // children_[0]->next();
  //         // if (children_[0]->next() == RC::RECORD_EOF)
  //         // {
  //         //   return RC::RECORD_EOF;
  //         // }
  //         // left_tuple_ = children_[0]->current_tuple();
  //         // LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());
  //         // children_[1]->close();
  //         // children_[1]->open(trx_);
  //       }
  //       return RC::SUCCESS;
  //     }
  //     right_tuple_ = nullptr;
  //   }
  //   if (children_[1]->next() == RC::RECORD_EOF)
  //   {
  //     LOG_INFO("right child eof");
  //     // children_[0]->next();
  //     // if (children_[0]->next() == RC::RECORD_EOF)
  //     // {
  //     //   return RC::RECORD_EOF;
  //     // }
  //     // left_tuple_ = children_[0]->current_tuple();
  //     // LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());
  //     children_[1]->close();
  //     children_[1]->open(trx_);
  //   }
  //   if (find)
  //   {
  //     return RC::SUCCESS;
  //   }
  //   // children_[1]->close();
  //   // children_[1]->open(trx_);
  //   if (children_[0]->next() != RC::SUCCESS)
  //   {
  //     return RC::RECORD_EOF;
  //   }
  //   left_tuple_ = children_[0]->current_tuple();
  //   LOG_INFO("left_tuple_ to string %s", left_tuple_->to_string().c_str());

  // }
  
  // if (!find)
  // {
  //   children_[0]->next();
  //   left_tuple_ = children_[0]->current_tuple();
  //   children_[1]->close();
  //   children_[1]->open(trx_);
  //   return next();
  // }


  // // 递归调用next
  // while (children_[0]->next() == RC::SUCCESS)
  // {
  //   LOG_INFO("JoinPhysicalOperator next called children_[0] next success");
  //   while (children_[1]->next() == RC::SUCCESS)
  //   {
  //     LOG_INFO("JoinPhysicalOperator next called children_[1] next success");
  //     // 判断是否满足join条件：std::unique_ptr<Expression> condition_;
  //     Tuple *left_tuple = children_[0]->current_tuple();
  //     LOG_INFO("left_tuple to string %s", left_tuple->to_string().c_str());
  //     Tuple *right_tuple = children_[1]->current_tuple();
  //     LOG_INFO("right_tuple to string %s", right_tuple->to_string().c_str());
  //     JoinedTuple joined_tuple;
  //     joined_tuple.set_left(left_tuple);
  //     joined_tuple.set_right(right_tuple);
  //     Value result;
  //     RC rc = condition_->get_value(joined_tuple, result);
  //     if (rc != RC::SUCCESS)
  //     {
  //       return rc;
  //     } 
  //     if (result.get_boolean())
  //     {
  //       joined_tuple_.set_left(left_tuple);
  //       joined_tuple_.set_right(right_tuple);
  //       // return RC::SUCCESS;
  //     }
  //   }
  //   children_[1]->close();
  //   children_[1]->open(trx_);
  // }
}

// 节点执行完成，清理左右子算子
RC JoinPhysicalOperator::close()
{
  if (!children_.empty())
  {
    children_[0]->close();
    children_[1]->close();
  }
  return RC::SUCCESS;
}

Tuple *JoinPhysicalOperator::current_tuple()
{
  return &joined_tuple_;
}
