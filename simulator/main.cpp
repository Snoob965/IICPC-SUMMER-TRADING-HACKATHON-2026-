#include "httplib.h"
#include <iostream>

using namespace httplib;
using namespace std;

int main() {
    Server svr;

    // This is the endpoint your Bot Fleet will bombard
    svr.Post("/order", [](const Request& req, Response& res) {
        
        // TODO: Plug your actual Limit Order Book logic in here!
        // For now, we simulate a fast execution to pass the validator
        cout << "ORDER: " << req.body << endl;
        cout << "FILL: Executed" << endl;
        
        res.set_content("{\"status\": \"filled\"}", "application/json");
    });

    // The sandbox container forwards port 8082 to internal port 8080
    cout << "SYSTEM: C++ Matching Engine booting on 0.0.0.0:8080..." << endl;
    
    // Listen on all network interfaces
    svr.listen("0.0.0.0", 8080);
    
    return 0;
}
