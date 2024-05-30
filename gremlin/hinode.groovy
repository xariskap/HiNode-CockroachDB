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
    } catch (RuntimeExceptioEMn e) {
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

GraphTraversal.metaClass.addA = { String attrName, String attrValue, String start = '', String end = ''->
   if (start.isEmpty()) {
        s = delegate.bytecode.StepInstructions
	s.each { inst ->
	if (inst.getOperator() == 'property' && inst.getArguments()[0] == 'start'){
	    start = inst.getArguments()[1]
	}}
    }

    if (end.isEmpty()) {
        s = delegate.bytecode.StepInstructions
	s.each { inst ->
	if (inst.getOperator() == 'property' && inst.getArguments()[0] == 'end'){
	    end = inst.getArguments()[1]
	}}
    }
    delegate.property(attrName, attrValue,'label', __.label(), 'start', start, 'end', end)
}

graph = TinkerGraph.open()
g = graph.traversal()

g.addV('Person').vid('1').lifetime('2010-01-01', '2099-12-31').addA('color', 'blue', '2010-01-01', '2011-01-01')
g.addV('Person').vid('2').lifetime('2011-01-01', '2089-12-31').addA('color', 'red')

g.insertE('PersonKnowsPerson', '1', '2').lifetime('2011-01-01', '2012-12-31').weight('1')

g.deleteV('1', '2012-12-31')
g.deleteE('1', '2', '2012-12-31')