GraphTraversal.metaClass.lifetime = { String startValue, String endValue ->
    delegate.property('start', startValue)
    delegate.property('end', endValue)
}

GraphTraversal.metaClass.vid = { String vidValue ->  
    try {
        if (delegate.bytecode.StepInstructions[0].toString().substring(0,4) != "addV") {
            throw new RuntimeException("Vid can be added only in vertices")
        } else {
            delegate.property('vid', vidValue)
        }
    } catch (RuntimeException e) {
        println("Error: ${e.message}")
    }
}

GraphTraversal.metaClass.weight = { String weightValue ->
    delegate.property('weight', weightValue)
}

GraphTraversalSource.metaClass.insertE = { String label, String sourceVidValue, String targetVidValue ->
    delegate.V().has('vid', sourceVidValue).addE(label).to(V().has('vid', targetVidValue))
}

GraphTraversalSource.metaClass.deleteV = { String vidValue, String endValue ->
    delegate.V().has('vid', vidValue).property('end', endValue)
}

GraphTraversalSource.metaClass.deleteE = { String sourceVidValue, String targetVidValue, String endValue ->
    delegate.V().has('vid', sourceVidValue).outE().where(inV().has('vid', targetVidValue)).property('end', endValue)
}

GraphTraversal.metaClass.addA = { String attrName, String attrValue ->
    delegate.property(attrName, attrValue)
}

graph = TinkerGraph.open()
g = graph.traversal()

g.addV('Person').vid('1').lifetime('2010', '2099').addA('firstname', 'xaris')
g.addV('Person').vid('2').lifetime('2011', '2099')

g.insertE('PersonKnowsPerson', '1', '2').lifetime('2010', '2099').weight('1')

g.deleteV('1', '2012')
g.deleteE('1', '2', '2012')

