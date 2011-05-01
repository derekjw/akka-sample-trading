package org.samples.trading.akkahawt

import org.samples.trading.akkabang._
import akka.dispatch.HawtDispatcher
import akka.dispatch.MessageDispatcher

class AkkaHawtTradingSystem extends AkkaBangTradingSystem {

  lazy val hawtDispatcher = new HawtDispatcher(false)

  override
  def createMatchingEngineDispatcher: Option[MessageDispatcher] = Option(hawtDispatcher)

  override def start {
    super.start


    for ((p, s) <- matchingEngines) {
      //      HawtDispatcher.pin(p)
      if (s != None) {
        //        HawtDispatcher.pin(s.get)
        HawtDispatcher.target(p, HawtDispatcher.queue(s.get))
      }
    }
  }


}
