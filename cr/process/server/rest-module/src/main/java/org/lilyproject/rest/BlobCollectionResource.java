/*
 * Copyright 2010 Outerthought bvba
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lilyproject.rest;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.NullInputStream;
import org.lilyproject.repository.api.Blob;
import org.lilyproject.util.io.Closer;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.io.OutputStream;

import static javax.ws.rs.core.Response.Status.*;

@Path("blob")
public class BlobCollectionResource extends RepositoryEnabled {

    @POST
    @Consumes("*/*")
    @Produces("application/json")
    public Response post(@Context HttpHeaders headers, InputStream is) {
        String lengthHeader = headers.getRequestHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);
        if (lengthHeader == null) {
            throw new ResourceException("Content-Length header is required for uploading blobs.", BAD_REQUEST.getStatusCode());
        }

        // TODO do we want the mediatype to include the parameters?
        String mediaType = headers.getMediaType().getType() + "/" + headers.getMediaType().getSubtype();

        long length = Long.parseLong(lengthHeader);
        Blob blob = new Blob(mediaType, length, null);

        if (length == 0 && is == null) {
            // Apparently when the length is 0, no InputStream is provided, therefore the following
            is = new NullInputStream(0);
        }

        OutputStream os = null;
        try {
            os = repository.getOutputStream(blob);
            IOUtils.copyLarge(is, os);
        } catch (Exception e) {
            throw new ResourceException("Error writing blob.", e, INTERNAL_SERVER_ERROR.getStatusCode());
        } finally {
            Closer.close(os);
        }

        return Response.ok().entity(blob).build();
    }

}
