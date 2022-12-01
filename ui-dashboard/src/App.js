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
            <img src={logo} className="App-logo" alt="logo" height="230px" width="280px"/>
            <div>
                <h1 className="App-header">OpenAtmos&copy; Air Quality Monitor</h1>
            </div>
            <div>
                <h2>Latest Measurements</h2>
                <AppStats/>
                <h3>System Status</h3>
                <AppStatus/>
                <h3>Audit Endpoints</h3>
                {rendered_endpoints}
            </div>
        </div>
    );
}



export default App;
