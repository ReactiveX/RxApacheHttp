/**
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package rx.apache.http.consumers;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.ContentDecoder;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.protocol.AbstractAsyncResponseConsumer;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.protocol.HttpContext;

import rx.Observable;
import rx.Observable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.apache.http.ObservableHttpResponse;
import rx.subjects.PublishSubject;
import rx.subscriptions.CompositeSubscription;

/**
 * {@link HttpAsyncResponseConsumer} for Transfer-Encoding:chunked
 * <p>
 * It will emit a byte[] via {@link Observer#onNext} for each non-empty line.
 */
class ResponseConsumerChunked extends AbstractAsyncResponseConsumer<HttpResponse> implements ResponseDelegate {

    private final static int                               BUFFER_SIZE    = 8192;

    private final Observer<? super ObservableHttpResponse> observer;
    private final PublishSubject<byte[]>                   contentSubject = PublishSubject.<byte[]> create();
    private final CompositeSubscription                    parentSubscription;

    public ResponseConsumerChunked(final Observer<? super ObservableHttpResponse> observer,
            CompositeSubscription parentSubscription) {
        this.observer = observer;
        this.parentSubscription = parentSubscription;
    }

    @Override
    protected final void onEntityEnclosed(final HttpEntity entity, final ContentType contentType) {
    }

    @Override
    protected final void onContentReceived(final ContentDecoder decoder, final IOControl ioctrl) throws IOException {
        if (parentSubscription.isUnsubscribed()) {
            ioctrl.shutdown();
        }

        byte[] data;
        data = new byte[BUFFER_SIZE];

        final int bytesRead;
        bytesRead = decoder.read(ByteBuffer.wrap(data));

        if (bytesRead > 0) {
            if (bytesRead == data.length) {
                contentSubject.onNext(data);

            } else {
                byte[] subset;
                subset = new byte[bytesRead];
                System.arraycopy(data, 0, subset, 0, bytesRead);

                contentSubject.onNext(subset);

            }

        }

        if (decoder.isCompleted()) {
            contentSubject.onCompleted();
        }
    }

    @Override
    protected void releaseResources() {
    }

    @Override
    public void _onContentReceived(ContentDecoder decoder, IOControl ioctrl) throws IOException {
        onContentReceived(decoder, ioctrl);
    }

    @Override
    public void _onEntityEnclosed(HttpEntity entity, ContentType contentType) throws IOException {
        onEntityEnclosed(entity, contentType);
    }

    @Override
    public HttpResponse _buildResult(HttpContext context) throws Exception {
        return buildResult(context);
    }

    @Override
    public void _releaseResources() {
        releaseResources();
    }

    @Override
    public void _onResponseReceived(HttpResponse aResponse) throws HttpException, IOException {
        onResponseReceived(aResponse);
    }

    @Override
    protected void onResponseReceived(HttpResponse aResponse) throws HttpException, IOException {
        // wrap the contentSubject so we can chain the Subscription between
        // parent and child
        Observable<byte[]> contentObservable = Observable.create(new OnSubscribe<byte[]>() {

            @Override
            public void call(Subscriber<? super byte[]> observer) {
                observer.add(parentSubscription);
                parentSubscription.add(contentSubject.subscribe(observer));
            }
        });
        observer.onNext(new ObservableHttpResponse(aResponse, contentObservable));
    }

    @Override
    protected HttpResponse buildResult(HttpContext aContext) throws Exception {
        // streaming results, so not returning anything here
        return null;
    }
}