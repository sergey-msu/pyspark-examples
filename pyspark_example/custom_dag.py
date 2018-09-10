import numpy as np


def run():

    input1 = InputNode()
    input2 = InputNode()

    n1 = LambdaNode(lambda x: x[0]+1)
    n2 = LambdaNode(lambda x: 2*x[0])
    input1.add_next(n1)
    input1.add_next(n2)

    n3 = LambdaNode(lambda x: x[0] + x[1])
    n1.add_next(n3)
    n2.add_next(n3)

    n4 = LambdaNode(lambda x: -x[0])
    n3.add_next(n4)

    n5 = LambdaNode(lambda x: x[0]**2)
    input2.add_next(n5)

    n6 = LambdaNode(lambda x: x[0])
    n5.add_next(n6)

    dag = DAG(inputs=[input1, input2], outputs=[n4, n6])
    dag.compile()
    res = dag.run(-2)

    print(res)


    return


class DAGNode():
    def __init__(self):
        self._prevs = []
        self._nexts = []
        self._accepted_values = None
        self._compiled = False

    def add_next(self, node):
        if not isinstance(node, DAGNode):
            raise ValueError('Node must be instance of'+str(DAGNode))
        if node in self._nexts or self in node._prevs:
            raise ValueError('Node has already been added')
        if node in self._prevs:
            raise ValueError('Node is already in _prevs collection')
        if self._compiled:
            raise ValueError('Node is already compiled')

        self._nexts.append(node)
        node._prevs.append(self)

        return


    def compile(self):
        if self._compiled:
            return

        self._accepted_values = { node: None for node in self._prevs }

        for node in self._nexts:
            node.compile()

        self._compiled = True


    def is_input(self):
        return len(self._prevs) == 0


    def is_output(self):
        return len(self._nexts) == 0


    def accept(self, node, value):
        if node not in self._prevs:
            raise ValueError('Can not accept value: node not in _prevs collection')
        if not self._compiled:
            raise ValueError('Compile node before use')
        if node not in self._accepted_values:
            raise ValueError('Compilation error: node not in _accepted_values dict')

        self._accepted_values[node] = value

        if None in self._accepted_values.values():
            return

        value = self.calc()
        if not self.is_output():
            for node in self._nexts:
                node.accept(self, value)

        self._accepted_values = { node: None for node in self._prevs }
        return


    def calc(self):
        if None in self._accepted_values.values():
            raise ValueError('Not all values were accepted from previous nodes')

        self.value = self._docalc()
        return self.value


    def _docalc():
        pass



class InputNode(DAGNode):
    def accept(self, value):
        if not self._compiled:
            raise ValueError('Compile node before use')

        if not self.is_output():
            for node in self._nexts:
                node.accept(self, value)
        return



class LambdaNode(DAGNode):
    def __init__(self, func):
        super(LambdaNode, self).__init__()
        self._func = func


    def _docalc(self):
        accepted = list(self._accepted_values.values())
        return self._func(accepted)



class DAG():
    def __init__(self, inputs, outputs):
        self._inputs  = inputs
        self._outputs = outputs
        return


    def compile(self):
        for node in self._inputs:
            node.compile()


    def run(self, value):
        for node in self._inputs:
            node.accept(value)

        return [node.value for node in self._outputs]
