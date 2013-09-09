/**
 * ====
 *     This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 *     Copyright (C) 2011-  SINTEF ICT
 *     Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 *     Module: net.modelbased.sensapp
 *
 *     SensApp is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU Lesser General Public License as
 *     published by the Free Software Foundation, either version 3 of
 *     the License, or (at your option) any later version.
 *
 *     SensApp is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *     GNU Lesser General Public License for more details.
 *
 *     You should have received a copy of the GNU Lesser General
 *     Public License along with SensApp. If not, see
 *     <http://www.gnu.org/licenses/>.
 * ====
 *
 * This file is part of SensApp [ http://sensapp.modelbased.net ]
 *
 * Copyright (C) 2012-  SINTEF ICT
 * Contact: SINTEF ICT <nicolas.ferry@sintef.no>
 *
 * Module: net.modelbased.sensapp.library.datastore
 *
 * SensApp is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * SensApp is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with SensApp. If not, see
 * <http://www.gnu.org/licenses/>.
 */
package net.modelbased.sensapp.library.datastore.specs

import org.specs2.mutable._
import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner
import net.modelbased.sensapp.library.datastore.specs.data._

@RunWith(classOf[JUnitRunner])
class MultiTypedSpecIT extends SpecificationWithJUnit {
  
  "MultiTyped data registry Specification unit".title
  
  "Push: a registry" should {
   
    "push a data" in new EmptyMultiTypeRegistry {
      val size = registry.size
      registry push data
      registry.size must_== (size+1)
    }
    
    "push in an idempotent way" in new FilledMultiTypeRegistry {
      val size = registry.size
      registry push m1
      registry.size must_== size
    }
    
    "support data update as simple push" in new EmptyMultiTypeRegistry {
      registry push data
      val identifier = registry identify data
      val dataPrime = MultiTypedData(data.n,-100)
      assert(identifier == (registry identify dataPrime))
      registry push dataPrime
      (registry pull identifier) must beSome.which{ _ == dataPrime }
    }
        
    "update data in place" in new EmptyMultiTypeRegistry {
      registry push data
      val size = registry.size
      val identifier = registry identify data
      val dataPrime = MultiTypedData(data.n,-100)
      assert(identifier == (registry identify dataPrime))
      registry push dataPrime
      registry.size must_== size
    }
  }
  
  "Pull: a registry" should {
    "pull an existing data" in new FilledMultiTypeRegistry {
      val identifier = registry identify m1
      (registry pull identifier) must beSome.which{ _ == m1 }
    }
    "not pull an unregistered data" in  new FilledMultiTypeRegistry {
      val identifier = registry identify unregistered
      (registry pull identifier) must beNone
    }
  }
  
  "Retrieve: a registry" should {
    "retrieve an empty list when nothing match" in new FilledMultiTypeRegistry {
      registry retrieve(List(registry identify unregistered)) must beEmpty
    }
    "retrieve a list of matched objects" in new FilledMultiTypeRegistry {
      registry retrieve(List(("v", 2))) must contain(m2,m3).only
    }
  }
  
  "Drop: a registry" should {
    "support element drop" in new FilledMultiTypeRegistry {
      registry drop m1
      registry pull(registry identify m1) must beNone
    }
    "support unexisting element drop silently" in new FilledMultiTypeRegistry {
      registry drop unregistered must not (throwAn[Exception])
    }
  }
  
  "DropAll: a registry" should {
    "be able to drop all the elements" in new FilledMultiTypeRegistry {
      registry dropAll()
      registry.size must_== 0
    }
  }
    
}

trait FilledMultiTypeRegistry extends Before {
  
  val registry = new MultiTypedRegistry()
  
  val m1 = MultiTypedData("m1",1)
  val m2 = MultiTypedData("m2",2)
  val m3 = MultiTypedData("m3",2)
  val unregistered = MultiTypedData("unregistered", -1)
  
  def before {
    registry dropAll()
    registry push m1
    registry push m2
    registry push m3
  }
}

trait EmptyMultiTypeRegistry extends Before {
  
  val data = MultiTypedData("primary",47)
  var other = MultiTypedData("other",47)
  
  val registry = new MultiTypedRegistry()

  def before { registry dropAll() }
}