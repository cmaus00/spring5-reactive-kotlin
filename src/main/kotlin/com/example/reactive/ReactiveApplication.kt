package com.example.reactive

import kotlinx.coroutines.experimental.delay
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.context.annotation.Bean
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import java.util.*
import kotlinx.coroutines.experimental.*
import kotlinx.coroutines.experimental.channels.ProducerScope
import kotlinx.coroutines.experimental.channels.ReceiveChannel
import kotlinx.coroutines.experimental.channels.consumeEach
import kotlinx.coroutines.experimental.channels.produce
import kotlinx.coroutines.experimental.reactive.awaitSingle
import kotlinx.coroutines.experimental.reactor.asFlux
import kotlinx.coroutines.experimental.reactor.flux
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.server.ServerRequest
import org.springframework.web.reactive.function.server.ServerResponse.*
import org.springframework.web.reactive.function.server.router
import reactor.core.publisher.Mono
import java.util.function.BiFunction
import kotlin.coroutines.experimental.CoroutineContext

@SpringBootApplication
class ReactiveApplication(val routeHandler: RouteHandler) {

    @Bean
    fun routes() = router {
        "/movies".nest {
            GET("/", routeHandler::movies)
            GET("/{id}", routeHandler::movie)
            GET("/{id}/events", routeHandler::movieStream)
            GET("/{id}/events2", routeHandler::movieStreamCoroutine)
        }
        "/users".nest {
            GET("/", routeHandler::userStream)
        }
    }

    @Bean
    fun cliRunner(repository: MovieRepository): CommandLineRunner {
        return CommandLineRunner {
            runBlocking {
                val savedMovie = repository.save(Movie(1, "Das Schweigen der Lämmer", listOf("Test"))).awaitSingle()
                println(savedMovie.toString())
            }
        }
    }
}


@Component
class RouteHandler(val streamService: MovieStreamService) {

    private val context = newSingleThreadContext("movieStream")

    fun movieStream(request: ServerRequest) = ok()
            .contentType(MediaType.TEXT_EVENT_STREAM)
            .body(streamService.streams(request.pathVariable("id").toInt()), MovieEvent::class.java)

    fun movieStreamCoroutine(request: ServerRequest) = ok()
            .contentType(MediaType.TEXT_EVENT_STREAM)
            .body(streamService.streamsCoroutine(request.pathVariable("id").toInt(), context).asFlux(context), MovieEvent::class.java)

    fun userStream(request: ServerRequest) = ok()
            .contentType(MediaType.TEXT_EVENT_STREAM)
            .body(streamService.userFlux(), String::class.java)

    fun movie(request: ServerRequest) = ok()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .body(streamService.byId(request.pathVariable("id").toInt()), Movie::class.java)

    fun movies(request: ServerRequest) = ok()
            .contentType(MediaType.APPLICATION_JSON_UTF8)
            .body(streamService.repository.findAll(), Movie::class.java)
}

fun main(args: Array<String>) {
    SpringApplication.run(ReactiveApplication::class.java, *args)
}

@Document(collection = "movies")
data class Movie(@Id var id: Int? = null, var name: String? = null, var cast: List<String>? = null)

data class MovieEvent(val date: Date, val user: String, val movie: Movie, val threadId: Long)

@Repository
interface MovieRepository : ReactiveCrudRepository<Movie, Int>

@Service
class MovieStreamService(val repository: MovieRepository) {

    fun logThread(msg: String) = println("$msg : ${Thread.currentThread().id}")

    fun byId(id: Int): Mono<Movie> {
        logThread("byId")
        return repository.findById(id)
    }

    fun streams(id: Int): Flux<MovieEvent> = movieEventStream(byId(id), userFlux())

    fun streamsCoroutine(id: Int, context: CoroutineContext) = movieEventCoroutine(byId(id), usersCoroutine(context), context)

    fun userFlux() = flux(Unconfined) { users() }
    fun usersCoroutine(context: CoroutineContext) = produce(context, 0) { users() }

    private fun movieEventStream(mono: Mono<Movie>, users: Flux<String>): Flux<MovieEvent> {
        return Flux.combineLatest(
                mono,
                users,
                BiFunction<Movie, String, MovieEvent> { m, u ->
                    createEvent(m, u)
                }
        )
    }

    fun movieEventCoroutine(mono: Mono<Movie>, users: ReceiveChannel<String>, context: CoroutineContext) = produce(context, 0) {
        logThread("movieEventCoroutine")
        users.consumeEach { user ->
            logThread("movieEventCoroutine send")
            send(createEvent(mono.awaitSingle(), user))
        }
    }


    fun createEvent(movie: Movie, user: String): MovieEvent {
        logThread("createEvent")
        return MovieEvent(Date(), user, movie, Thread.currentThread().id)
    }


    /**
     * implemented as an extension function of ProducerScope so that it may be used both in flux and produce contexts
     */
    suspend fun ProducerScope<String>.users() {
        logThread("userFunction")
        val users = listOf("kenny", "waldo", "deb", "narf")

        while (true) {
            delay(200)
            val index = Random().nextInt(users.size)
            logThread("userFunction send")
            send(users.get(index))
        }
    }
}




