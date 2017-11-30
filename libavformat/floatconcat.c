/*
 * Floating Concat URL protocol
 * Copyright (c) 2016 Thilo Fischer, KST innovations GmbH
 * Based on the concat.c protocol from Steve Lhomme, Wolfram Gloger, 2010 Michele Orrù
 *
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

#include <stdbool.h>
#include <unistd.h>
#include <ctype.h>

#include "libavutil/avstring.h"
#include "libavutil/mem.h"

#include "avformat.h"
#include "url.h"

struct flccat_nodes {
    URLContext *uc;                ///< node's URLContext
    int64_t     size;              ///< url filesize
};


#define FILENAME_FMT_MAX_LENGTH 1024

struct flccat_data {
    struct flccat_nodes current;  ///< currently read node
    int idx;
    char filename_fmt[FILENAME_FMT_MAX_LENGTH];
    int64_t segment_start; ///< accumulated size of all nodes that go before current
};

static const char *filename_from_idx(char *filename, const char *filename_fmt, int idx) {
    snprintf(filename, FILENAME_FMT_MAX_LENGTH, filename_fmt, idx);
    return filename;
}

static bool file_exist_filename(const char *filename) {
    return access(filename, R_OK) == 0;
}

static int64_t update_current_size(struct flccat_nodes *current) {
    int64_t result = ffurl_size(current->uc);
    if (result < 0) {
        // failure
        return result;
    } else {
        current->size = result;
        return result;
    }
}


static int open_filename(URLContext *h, const char *filename, int flags) {
    int err = 0;
    struct flccat_data *data = h->priv_data;
     
    /* creating URLContext */
    if ((err = ffurl_open(&data->current.uc, filename, flags,
			  &h->interrupt_callback, NULL)) < 0) {
	return err;
    }

    /* creating size */
    if (update_current_size(&data->current) < 0) {
	ffurl_close(data->current.uc);
	return AVERROR(ENOSYS);
    }

    return 0;
}

static int open_idx(URLContext *h, int flags) {
    struct flccat_data  *data = h->priv_data;
    char filename[FILENAME_FMT_MAX_LENGTH];
    filename_from_idx(filename, data->filename_fmt, data->idx);
    return open_filename(h, filename, flags);
}

static int close_current(URLContext *h) {
    struct flccat_data  *data = h->priv_data;
    return ffurl_close(data->current.uc);
}

static av_cold int flccat_close(URLContext *h);

static av_cold int flccat_open(URLContext *h, const char *uri, int flags)
{
    int err = 0;
    size_t uri_len, num_begin, num_end;
    struct flccat_data  *data = h->priv_data;
    const char *c_iter;

    // initialize data's elements to zero
    data->current.uc = NULL;
    data->current.size = 0;
    data->filename_fmt[0] = '\0';
    data->idx = 0;
    
    // set data's elements to its initial values
    data->segment_start = 0;

    av_strstart(uri, "flccat:", &uri);

    /* handle input */
    if (!*uri)
        return AVERROR(ENOENT);

    uri_len = strnlen(uri, FILENAME_FMT_MAX_LENGTH-1);

    // set c_iter to the last character of uri
    c_iter = uri + uri_len - 1;

    while (!isdigit(*c_iter) && c_iter > uri) {
      --c_iter;
    }
    // set num_end to the index of the first non-digit character after the last digit in uri
    num_end = c_iter - uri + 1;
    
    while (isdigit(*c_iter) && c_iter > uri) {
      --c_iter;
    }
    // set num_begin to the index of the first digit character of the last sequence of digits in uri    
    num_begin = c_iter - uri + 1;
    data->idx = strtol(c_iter + 1, NULL, 10);

    if (num_begin != num_end) {
        av_strlcpy(data->filename_fmt, uri, num_begin + 1);
        av_strlcpy(data->filename_fmt + num_begin, "%d", 2 + 1);
        av_strlcpy(data->filename_fmt + num_begin + 2, uri + num_end, uri_len - num_end + 1);
    } else {
        return AVERROR(ENOENT);
    }

    err = open_idx(h, flags);
    
    if (err < 0)
        flccat_close(h);

    return err;
}

static av_cold int flccat_close(URLContext *h)
{
    struct flccat_data  *data  = h->priv_data;
    int err = ffurl_close(data->current.uc);
    return err < 0 ? -1 : 0;
}

static int64_t progress(URLContext *h, int direction);

static int64_t go_forward(URLContext *h) {
    return progress(h, 1);
}

static int64_t go_backward(URLContext *h) {
    return progress(h, -1);
}

/// Switch to previous or next file, if such file exists.
/// @param direction must be either 1 or -1. Go forward if 1, go backward if -1.
/// @return 1 if successfully switched to another file, 0 if not switched because no such file exists, an error code < 0 if some error occured.
static int64_t progress(URLContext *h, int direction) {
    int64_t err = 0;
    struct flccat_data  *data  = h->priv_data;
    char filename[FILENAME_FMT_MAX_LENGTH];

    assert(direction == 1 || direction == -1);

    filename_from_idx(filename, data->filename_fmt, data->idx + direction);

    if (file_exist_filename(filename)) {
        const int64_t size_first = ffurl_size(data->current.uc);//data->current.size;
        if ((err = close_current(h)) < 0) return err;
        if ((err = open_filename(h, filename, data->current.uc->flags)) < 0) return err; // FIXME race condition: might fail if file gets deleted in between file_exist and open_filename because of ringbuffer mechanism when going backwards
        data->idx += direction;
        if (direction > 0) {
            // add size of previous
            data->segment_start += size_first;
        } else {
            // subtract size of current
            data->segment_start -= data->current.size;	
        }
        if ((err = ffurl_seek(data->current.uc, 0, SEEK_SET)) < 0)
            return err;
        return 1;
    } else {
        return 0;
    }
}

static int64_t activate_current(URLContext *h) {
    struct flccat_data  *data  = h->priv_data;
    int64_t result = ffurl_seek(data->current.uc, 0, SEEK_SET);    
    return result;
}

static int flccat_read(URLContext *h, unsigned char *buf, int size)
{
    int err, result, total = 0;
    struct flccat_data  *data  = h->priv_data;

    while (size > 0) {
        result = ffurl_read(data->current.uc, buf, size);
        if (result < 0)
            return total ? total : result;
        if (result == 0 || result < size) {
	    if ((err = go_forward(h)) < 0) {
		return err;
	    }
#if 0 // FIXME handle end of file correctly
            if (err == 0) {
                // reached end of last file
                int retval = total > 0 ? total : AVERROR(EIO);
                return retval;
            }
#endif
            if ((err = activate_current(h)) < 0) {
		return err;
            }
        }
        total += result;
        buf   += result;
        size  -= result;
    }
    return total;
}

/// @return the absolute position seeked to
static int64_t seek_relative(URLContext *h, int64_t pos) {
    struct flccat_data  *data  = h->priv_data;
    int64_t offset = ffurl_seek(data->current.uc, 0, SEEK_CUR);
    while (offset + pos < 0) {
        // pos is negative and reaches back into a previous file => go backward
        int64_t progress = go_backward(h);
        switch (progress) {
        case 0:
            // can't go further
            offset = ffurl_seek(data->current.uc, 0, SEEK_SET);
            if (offset < 0) {
                // error code
                return offset;
            } else {
                // seek position
                return data->segment_start + offset;
            }
        case 1:
            pos += data->current.size;
            break;
        default:
            return progress;
        }
    }
    while (offset + pos > data->current.size) {
        // pos is positive and reaches into a successive file => go forward
        int size = data->current.size;
        int64_t progress = go_forward(h);
        switch (progress) {
        case 0:
            // can't go further
            // file might have grown
            update_current_size(&data->current);
            offset = ffurl_seek(data->current.uc, -1, SEEK_END);
            if (offset < 0) {
                // error code
                return offset;
            } else {
                // seek position
                return data->segment_start + offset;
            }
        case 1:
            pos -= size;
            break;
        default:
            return progress;
        }
    }
    
    offset = ffurl_seek(data->current.uc, offset + pos, SEEK_SET);
    if (offset < 0) {
        // error code
        return offset;
    } else {
        // seek position
        return data->segment_start + offset;
    }
}

static int64_t flccat_seek(URLContext *h, int64_t pos, int whence)
{
    struct flccat_data  *data  = h->priv_data;

    switch (whence) {
#if 0 // XXX
    case AVSEEK_SIZE:
        return 100000000;
        //return 1000000000L;
        //return INT64_MAX;
#endif
    case SEEK_END:
    {
        int64_t progress = 0;
        do {
            progress = go_forward(h);
        } while (progress > 0);
        if (progress < 0) {
            // error occured
            return progress;
        }
        // reached last file
        // seek to its beginning (seek_relative assumes valid seek position in current file)
        if ((progress = activate_current(h)) < 0) return progress;
        // seek relative to its end
        return seek_relative(h, data->current.size + pos);
    }
    case SEEK_CUR:
        return seek_relative(h, pos);
    case SEEK_SET:
    {
        int64_t offset = ffurl_seek(data->current.uc, 0, SEEK_CUR);
        int64_t current_pos = data->segment_start + offset;
        return seek_relative(h, pos - current_pos);
    }
    default:
        return AVERROR(EINVAL);
    }
}

const URLProtocol ff_floatconcat_protocol = {
    .name           = "flccat",
    .url_open       = flccat_open,
    .url_read       = flccat_read,
    .url_seek       = flccat_seek,
    .url_close      = flccat_close,
    .priv_data_size = sizeof(struct flccat_data),
};

