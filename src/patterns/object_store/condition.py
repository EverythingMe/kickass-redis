#Copyright 2012 Do@. All rights reserved.
#
#Redistribution and use in source and binary forms, with or without modification, are
#permitted provided that the following conditions are met:
#
#   1. Redistributions of source code must retain the above copyright notice, this list of
#      conditions and the following disclaimer.
#
#   2. Redistributions in binary form must reproduce the above copyright notice, this list
#      of conditions and the following disclaimer in the documentation and/or other materials
#      provided with the distribution.
#
#THIS SOFTWARE IS PROVIDED BY Do@ ``AS IS'' AND ANY EXPRESS OR IMPLIED
#WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
#FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> OR
#CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
#CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON
#ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
#NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF
#ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
#The views and conclusions contained in the software and documentation are those of the
#authors and should not be interpreted as representing official policies, either expressed
#or implied, of Do@.

__author__ = 'dvirsky'

class Condition(object):

    """
    This class represents a condition to fetch objects
    conditions are constructed with a dictionary of field values or ranges, and paging offsets
    """
    class ConditionType(object):
        def __repr__(self):

            return '%s (%s)' % (self.__class__.__name__, self.__dict__)
    """Condition types"""
    class Is(ConditionType):
        """
        Just like ==
        """
        def __init__(self, value):
            self.value = value

    class In(ConditionType):
        """
        Valus is in a multiple option
        """
        def __init__(self, *values):
            raise NotImplementedError("Not implemented yet in any key")
            self.values = values

    class Between(ConditionType):
        """
        Range condition
        """
        def __init__(self, min, max):
            self.min = min
            self.max = max

    def __init__(self, fieldsAndValues, paging = None):
        """
        @param fieldsAndValues a dictionary of fields and their requested values
        @param paging a tuple of (offset, num) to get
        """

        self.fieldsAndValues = fieldsAndValues
        self.paging = paging

    def getValuesFor(self, *fields):

        return [self.fieldsAndValues[f] for f in fields]

    def __repr__(self):

        return 'Condition(%s, paging: %s)' % (self.fieldsAndValues, self.paging)