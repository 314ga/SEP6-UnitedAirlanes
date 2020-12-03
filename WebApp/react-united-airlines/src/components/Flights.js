//bootstrap imports
import 'bootstrap/dist/css/bootstrap.min.css';
import Jumbotron from 'react-bootstrap/Jumbotron'
import Button from 'react-bootstrap/Button'
import Accordion from 'react-bootstrap/Accordion'
//variable imports





function Flights() {

    return (
        <div >
            <Jumbotron fluid id="weather-header" className="BgStyle" >
                <div className="col-md-6 mx-auto text-white text-center">
                    <h4 className="h4 mt-2 mb-5">Weather Warning</h4>

                </div>

            </Jumbotron>
            <Accordion>
                <div className="row text-center mb-5">
                    <div className="col-md-5"><Accordion.Toggle as={Button} className="outline-link" eventKey="1">SEE WEATHER WARNINGS WITH SOCKETS</Accordion.Toggle></div>
                    <div className="offset-md-2 col-md-5 text-center mb-2"><Accordion.Toggle as={Button} className="outline-link " eventKey="0">SEE WEATHER WARNINGS WITH POLLING</Accordion.Toggle></div>

                </div>

            </Accordion>
            <div>

            </div>

        </div>

    )


}
export default Flights;
