# MersenneManager
Automatic fetching of assignments and submission of results for Mfacto and clLucas with GIMPS and gpu72 projects.

I wrote these tools to automate assignments/results for multiple instances of mfacto and clLucas.  This allows the settings to be tailored to each instance/GPU.  

# Installation

#### Pre-compiled Executables
[Release binaries](https://github.com/Kunde21/MersenneManager/releases) are available for many common Operating Systems and Architectures.  

#### From Source
A Go installation is required to build from source.  

    go get github.com/Kunde21/MersenneManager/TFmanager
    go get github.com/Kunde21/MersenneManager/LLmanager

# Configure and Run
Initial configuration is as simple as running the manager with the `-w` flag.  This will write the defaults to its respective setting file (yaml format).  

This configuration is loaded at program start, so changes require re-starting the program.  Additionally, account and device (1st device only) options can be overridden via command-line options.  Use `-h` to see the flags and options available.

# Future Plans
 - Create a combined manager that pulls the capabilities and configurations of both into a single program.  
 - Run Mfacto and clLucas processes from within the manager programs, with crash recovery logic.
 - Build out a FFT size to allow configuration to specify the best sizes to use for each clLucas instance (2-, 3-, 5-, and/or 7- smooth), then pass in the FFT size with the `-f` option in clLucas.
