import logo from './logo.png';
import './App.css';

import EndpointAudit from './components/EndpointAudit'
import AppStats from './components/AppStats'
import AppStatus from './components/AppStatus'

function App() {

    const endpoints = ["environment", "temperature"]

    const rendered_endpoints = endpoints.map((endpoint) => {
        return <EndpointAudit key={endpoint} endpoint={endpoint}/>
    })

    return (
        <div className="App">
            <img src={logo} className="App-logo" alt="logo" height="200px" width="200px"/>
            <div>
                <h2>OpenAtmos&copy; Air Quality Monitor</h2>
            </div>
            <div>
                <h3>Latest Measurements</h3>
                <AppStats/>
                <h4>Audit Endpoints</h4>
                {rendered_endpoints}
                <h4>System Health</h4>
                <AppStatus/>
            </div>
        </div>
    );

}



export default App;
