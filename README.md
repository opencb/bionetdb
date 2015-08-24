# BioNetDB

BioNetDB implements a storage engine to work with biological networks using a NoSQL graph database. BioNetDB integrates relevant biological networks information from well-known data sources such as Reactome. All this data can be queried through command line interface.

### Documentation
You can find BioNetDB documentation and tutorials at: https://github.com/opencb/bionetdb/wiki.

### Issues Tracking
You can report bugs or request new features at [GitHub issue tracking](https://github.com/opencb/bionetdb/issues).

### Release Notes and Roadmap
Releases notes are available at [GitHub releases](https://github.com/opencb/bionetdb/releases).

Roadmap is available at [GitHub milestones](https://github.com/opencb/bionetdb/milestones).

### Versioning
BioNetDB is versioned following the rules from [Semantic versioning](http://semver.org/).

### Maintainers
We recommend to contact BioNetDB developers by writing to BioNetDB mailing list opencb@googlegroups.com. The main developers and maintainers are:
* Ignacio Medina (im411@cam.ac.uk) (_Founder and Project Leader_)
* Daniel Perez-Gil  (daniel.perez@incliva.es)
* Pedro Furio-Tari  (pfurio@cipf.es)

##### Contributing
BioNetDB is an open-source and collaborative project. We appreciate any help and feedback from users, you can contribute in many different ways such as simple bug reporting and feature request. Depending on your skills you are more than welcome to develop client tools, new features or even fixing bugs.

# How to build 
BioNetDB is mainly developed in Java and it uses [Apache Maven](http://maven.apache.org/) as building tool. BioNetDB requires Java 8+ and others OpenCB Java dependencies that can be found in [Maven Central Repository](http://search.maven.org/).

Stable releases are merged and tagged at **_master_** branch. You are encouraged to use latest stable release for production. Current active development is carried out at **_develop_** branch, only compilation is guaranteed and bugs are expected, use this branch for development or for testing new functionalities. Only dependencies of **_master_** branch are ensured to be deployed at [Maven Central Repository](http://search.maven.org/), **_develop_** branch may require users to download and install other active OpenCB repositories:
* _biodata_: https://github.com/opencb/biodata (branch 'develop')
* _datastore_: https://github.com/opencb/datastore (branch 'develop')

### Cloning
BioNetDB is an open-source and free project, you can download **_develop_** branch by executing:

    $ git clone https://github.com/opencb/bionetdb.git

Latest stable release at **_master_** branch can be downloaded executing:

    $ git clone -b master https://github.com/opencb/bionetdb.git

### Build
You can build BioNetDB by executing the following command from the root of the cloned repository:
  
    $ mvn clean install -DskipTests
    
Remember that **_develop_** branch dependencies are not ensured to be deployed at Maven Central, you may need to clone and install **_develop_** branches from OpenCB _biodata_ and _datastore_ repositories.

### Testing
You can run the unit tests using Maven or your favorite IDE.

### Command Line Interface (CLI)
If the build process has gone well you should get an integrated help by executing:

    $ ./bin/bionetdb.sh --help

You can find more detailed documentation and tutorials at: https://github.com/opencb/bionetdb/wiki.
