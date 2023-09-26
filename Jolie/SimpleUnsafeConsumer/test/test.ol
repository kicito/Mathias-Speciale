from assertions import Assertions
from console import Console
from string-utils import StringUtils
from ..SimpleConsumer import SimpleConsumer as MainService

interface MyTestInterface {
RequestResponse:
	///@Test
	testMain(void)(void) throws TestFailed(string)
}

service main( ) {

	embed Assertions as assertions
	embed Console as console
	embed StringUtils as stringUtils
    embed MainService as mainService
    
	execution: sequential

	inputPort Input {
		location: "local"
		interfaces: MyTestInterface
	}

    main{
		[ testMain()() {
			scope(test){
				install( default => 
					throw( TestFailed, "expected World" )
				)
                response = hello@mainService()
				equals@assertions({
					actual = response
					expected = "World"
				})()
			}
		} ]
    }
}