import scala.collection.immutable.HashSet

class HashSetUtil[Long] {

	def removeRepeat(secondIds:HashSet[Long],firstIds: HashSet[Long]):HashSet[Long]={
		secondIds.filter(id => firstIds.contains(id)==false)
	}

}
