#!/usr/bin/python
"""
   Copyright (c) 2013, Yue.YE, ACT, Beihang University.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import rule

class Op:
    info = None
    map_phase = []
    reduce_phase = []
    child_list = []
    parent = None

    def __init__(self):
	pass

    def merge(self, op, rule_type)
	rule.Rule(rule_type).merge(self, op)
	
class Selectivity:
    sel = 0
    pro = 0

    def __init__(self, sel, pro):
	self.sel = sel
	self.pro = pro

class SpjOp(Op):
    is_sp = False

    def __init__(self):
	pass

class SpjeOp(Op):
    
    def __init__(self):
	pass
