'use strict';

var rxjs = require('rxjs');
var operators = require('rxjs/operators');

/**
 * Lazy loads Firebase Performance monitoring and returns the instance as
 * an observable
 * @param app
 * @returns Observable<FirebasePerformance>
 */
var getPerformance$ = function (app) { return rxjs.from(import('firebase/performance').then(function (module) { return module.getPerformance(app); })); };
/**
 * Creates an observable that begins a trace with a given id. The trace is ended
 * when the observable unsubscribes. The measurement is also logged as a performance
 * entry.
 * @param traceId
 * @returns Observable<void>
 */
var trace$ = function (traceId) {
    if (typeof window !== 'undefined' && window.performance) {
        var entries = window.performance.getEntriesByName(traceId, 'measure') || [];
        var startMarkName_1 = "_".concat(traceId, "Start[").concat(entries.length, "]");
        var endMarkName_1 = "_".concat(traceId, "End[").concat(entries.length, "]");
        return new rxjs.Observable(function (emitter) {
            window.performance.mark(startMarkName_1);
            emitter.next();
            return {
                unsubscribe: function () {
                    window.performance.mark(endMarkName_1);
                    window.performance.measure(traceId, startMarkName_1, endMarkName_1);
                },
            };
        });
    }
    else {
        return rxjs.EMPTY;
    }
};
/**
 * Creates a function that creates an observable that begins a trace with a given id. The trace is ended
 * when the observable unsubscribes. The measurement is also logged as a performance
 * entry.
 * @param name
 * @returns (source$: Observable<T>) => Observable<T>
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
var trace = function (name) { return function (source$) { return new rxjs.Observable(function (subscriber) {
    var traceSubscription = trace$(name).subscribe();
    return source$.pipe(operators.tap({
        next: function () { return traceSubscription.unsubscribe(); },
        error: function () { },
        complete: function () { return traceSubscription.unsubscribe(); },
    })).subscribe(subscriber);
}); }; };
/**
 * Creates a function that creates an observable that begins a trace with a given name. The trace runs until
 * a condition resolves to true and then the observable unsubscribes and ends the trace.
 * @param name
 * @param test
 * @param options
 * @returns (source$: Observable<T>) => Observable<T>
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
var traceUntil = function (name, test, options) { return function (source$) { return new rxjs.Observable(function (subscriber) {
    var traceSubscription = trace$(name).subscribe();
    return source$.pipe(operators.tap({
        next: function (a) { return test(a) && traceSubscription.unsubscribe(); },
        complete: function () { return options && options.orComplete && traceSubscription.unsubscribe(); },
    })).subscribe(subscriber);
}); }; };
/**
 * Creates a function that creates an observable that begins a trace with a given name. The trace runs while
 * a condition resolves to true. Once the condition fails the observable unsubscribes
 * and ends the trace.
 * @param name
 * @param test
 * @param options
 * @returns (source$: Observable<T>) => Observable<T>
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
var traceWhile = function (name, test, options) { return function (source$) { return new rxjs.Observable(function (subscriber) {
    var traceSubscription;
    return source$.pipe(operators.tap({
        next: function (a) {
            if (test(a)) {
                traceSubscription = traceSubscription || trace$(name).subscribe();
            }
            else {
                if (traceSubscription) {
                    traceSubscription.unsubscribe();
                }
                traceSubscription = undefined;
            }
        },
        complete: function () { return options && options.orComplete && traceSubscription && traceSubscription.unsubscribe(); },
    })).subscribe(subscriber);
}); }; };
/**
 * Creates a function that creates an observable that begins a trace with a given name. The trace runs until the
 * observable fully completes.
 * @param name
 * @returns (source$: Observable<T>) => Observable<T>
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
var traceUntilComplete = function (name) { return function (source$) { return new rxjs.Observable(function (subscriber) {
    var traceSubscription = trace$(name).subscribe();
    return source$.pipe(operators.tap({
        complete: function () { return traceSubscription.unsubscribe(); },
    })).subscribe(subscriber);
}); }; };
/**
 * Creates a function that creates an observable that begins a trace with a given name.
 * The trace runs until the first value emits from the provided observable.
 * @param name
 * @returns (source$: Observable<T>) => Observable<T>
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
var traceUntilFirst = function (name) { return function (source$) { return new rxjs.Observable(function (subscriber) {
    var traceSubscription = trace$(name).subscribe();
    return source$.pipe(operators.tap({
        next: function () { return traceSubscription.unsubscribe(); },
    })).subscribe(subscriber);
}); }; };

exports.getPerformance$ = getPerformance$;
exports.trace = trace;
exports.traceUntil = traceUntil;
exports.traceUntilComplete = traceUntilComplete;
exports.traceUntilFirst = traceUntilFirst;
exports.traceWhile = traceWhile;
//# sourceMappingURL=index.cjs.js.map
