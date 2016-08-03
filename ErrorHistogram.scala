package com.marin.util
import org.apache.spark.rdd.RDD

/*
@author: Krishnan Raman
Common Usage: 
Decile Histogram: ErrorHistogram.decileHistogram(predsAndObs)
Quartile Histogram: ErrorHistogram.quartileHistogram(predsAndObs)
*/

object ErrorHistogram {

  def to3D(a:Double) = toND(a,3)
  def to2D(a:Double) = toND(a,2)

  def toND(a:Double, N:Int) = {
    assert(N >0 && N < 6)
    val pow = math.pow(10,N).toDouble
    (a*pow).toInt/pow
  }

  case class Fourple(a:Int,b:Int,c:Int,d:Int) {
    def +(y:Fourple) = Fourple(a + y.a, b+y.b, c+y.c, d+y.d)
    def mkString:String = s"${a}%,${b}%,${c}%,${d}%"
    def fromList(x:List[Int]) = Fourple(x(0),x(1),x(2),x(3))
    def normalize:Fourple = {
      val total = a+b+c+d
      fromList(List(a,b,c,d).map {x => x*100/total})
    }
  }

  def quartileHistogram(predsAndObs:RDD[(Double, Double)]):String = {
    /* (a,b,c,d) => a% within first quartile, b% within second, c% within 3rd, d% beyond 3rd
    given observed = y and predicted = x,
    q = 100*abs(y-x)/y
    if q within 25% then add to a
    else if q within 50% then add to b
    else if q within 75% then add to c
    else add to d

    after computing all, normalize such that a+b+c+d = 100
    Obviously we want (100,0,0,0) ( ie. all predictions within 25% of observed)
    We never want (0,0,0,100) (ie. all predictions outside 75% of observed)
    */
    val tuples:RDD[Fourple] = predsAndObs.map{ xy=> 
      val (x,y) = xy
      val q = if (y != 0) 100 * math.abs(y-x)/y 
              else if (y == x) 0 
              else 100

      if (q < 25) Fourple(1,0,0,0)
      else if (q < 50) Fourple(0,1,0,0)
      else if (q < 75) Fourple(0,0,1,0)
      else Fourple(0,0,0,1)
    }

    tuples
    .reduce{(a,b) => a+b}
    .normalize
    .mkString
  }

  case class Decile(x:List[Int]) {
    def +(y:Decile) = Decile(this.x.zip(y.x).map{ i=> i._1 + i._2 })
    def mkString:String = x.mkString("%,") + "%"
    def normalize:Decile = {
      val total = x.sum
     Decile( x.map {i => i*100/total})
    }
  }

  def decileHistogram(predsAndObs:RDD[(Double, Double)]):String = {
    val tuples:RDD[Decile] = predsAndObs.map{ xy=> 
      val (x,y) = xy
      val q = if (y != 0) 100 * math.abs(y-x)/y 
              else if (y == x) 0 
              else 100

      val idx = {
        val i = math.floor(q/10.0).toInt
        if (i > 9) 9 else i
      }
      
      assert(idx >= 0 && idx < 10)
      val decile = Array.fill[Int](10)(0)
      decile(idx) = 1

      Decile(decile.toList)
    }

    tuples
    .reduce{(a,b) => a+b}
    .normalize
    .mkString
  }
}
