## What is this? ##
This is the java source behind an R (r-project.org) extension called [r-message-queue](http://r-forge.r-project.org/projects/r-message-queue/) that allows R to read and write to message queues to facilitate batch processing.

This isn't a runnable jar or library, rather the JAR that should appear within the [messageQueue](http://r-forge.r-project.org/projects/r-message-queue/)/pkg/inst/java directory with R references using [rJava](http://www.rforge.net/rJava/)

## What Queues are Supported? ##
Currently, [ActiveMQ](http://activemq.apache.org/) and [RabbitMQ](http://www.rabbitmq.com/) are supported.

## Why? ##
The idea is to provide this basic functionality such that R doesn't need to deal with the specifics of queues or their underlying implementation differences.  Provides a simple way to PUT a text message on a queue and GET a text message from a queue.

## What now? ##
Feel free to extend in any way that you want.


## Missing ##
Unit tests.