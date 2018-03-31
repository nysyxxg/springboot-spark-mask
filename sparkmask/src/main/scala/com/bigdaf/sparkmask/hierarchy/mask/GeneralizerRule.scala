package com.bigdaf.sparkmask.hierarchy.mask

trait GeneralizerRule[IN,OUT] extends Serializable{
  def mask(input:IN):OUT
}
