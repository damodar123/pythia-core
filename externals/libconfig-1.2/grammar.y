/* -*- mode: C -*- */
/* ----------------------------------------------------------------------------
   libconfig - A structured configuration file parsing library
   Copyright (C) 2005-2007  Mark A Lindner
 
   This file is part of libconfig.
    
   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public License
   as published by the Free Software Foundation; either version 2.1 of
   the License, or (at your option) any later version.
    
   This library is distributed in the hope that it will be useful, but
   WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.
    
   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA
   ----------------------------------------------------------------------------
*/

%defines
%output="y.tab.c"
%pure-parser
%lex-param{void *scanner}
%parse-param{void *scanner}
%parse-param{struct parse_context *ctx}

%{
#include <string.h>
#include <stdlib.h>
#include "libconfig.h"
#ifdef WIN32

// prevent warnings about redefined malloc/free in generated code:
#ifndef _STDLIB_H
#define _STDLIB_H
#endif
  
#include <malloc.h>
#endif
#include "private.h"

//these delcarations are provided to suppress compiler warnings
extern int libconfig_yylex();
extern int libconfig_yyget_lineno();
  
static const char *err_array_elem_type = "mismatched element type in array";
static const char *err_duplicate_setting = "duplicate setting name";

#define IN_ARRAY() \
  (ctx->parent && (ctx->parent->type == CONFIG_TYPE_ARRAY))

#define IN_LIST() \
  (ctx->parent && (ctx->parent->type == CONFIG_TYPE_LIST))

#define CAPTURE_PARSE_POS(S)                                    \
  (S)->line = (unsigned int)libconfig_yyget_lineno(scanner)

void libconfig_yyerror(void *scanner, struct parse_context *ctx,
                      char const *s)
{
  ctx->config->error_line = libconfig_yyget_lineno(scanner);
  ctx->config->error_text = s;
}


%}

%union
{
  long ival;
  double fval;
  char *sval;
}

%token <ival> TOK_BOOLEAN TOK_INTEGER TOK_HEX
%token <fval> TOK_FLOAT
%token <sval> TOK_STRING TOK_NAME
%token TOK_EQUALS TOK_NEWLINE TOK_ARRAY_START TOK_ARRAY_END TOK_LIST_START TOK_LIST_END TOK_COMMA TOK_GROUP_START TOK_GROUP_END TOK_END TOK_GARBAGE

%%

configuration:
    /* empty */
  | setting_list
  ;

setting_list:
    setting
  | setting_list setting
  ;

setting_list_optional:
    /* empty */
  | setting_list
  ;

setting:
  TOK_NAME
  {
    ctx->setting = config_setting_add(ctx->parent, $1, CONFIG_TYPE_NONE);
    free($1);
  
    if(ctx->setting == NULL)
    {
      libconfig_yyerror(scanner, ctx, err_duplicate_setting);
      YYABORT;
    }
    else
    {
      CAPTURE_PARSE_POS(ctx->setting);
    }
  }

  TOK_EQUALS value TOK_END
  ;
  
array:
  TOK_ARRAY_START
  {
    if(IN_LIST())
    {
      ctx->parent = config_setting_add(ctx->parent, NULL, CONFIG_TYPE_ARRAY);
      CAPTURE_PARSE_POS(ctx->parent);
    }
    else
    {
      ctx->setting->type = CONFIG_TYPE_ARRAY;
      ctx->parent = ctx->setting;
      ctx->setting = NULL;
    }
  }
  simple_value_list_optional
  TOK_ARRAY_END
  {
    if(ctx->parent)
      ctx->parent = ctx->parent->parent;    
  }
  ;

list:
  TOK_LIST_START
  {
    if(IN_LIST())
    {
      ctx->parent = config_setting_add(ctx->parent, NULL, CONFIG_TYPE_LIST);
      CAPTURE_PARSE_POS(ctx->parent);
    }
    else
    {
      ctx->setting->type = CONFIG_TYPE_LIST;
      ctx->parent = ctx->setting;
      ctx->setting = NULL;
    }
  }
  value_list_optional
  TOK_LIST_END
  {
    if(ctx->parent)
      ctx->parent = ctx->parent->parent;    
  }
  ;

value:
    simple_value
  | array
  | list
  | group
  ;

simple_value:
    TOK_BOOLEAN
  {
    if(IN_ARRAY() || IN_LIST())
    {
      config_setting_t *e = config_setting_set_bool_elem(ctx->parent, -1,
                                                         (int)$1);
      
      if(! e)
      {
        libconfig_yyerror(scanner, ctx, err_array_elem_type);
        YYABORT;
      }
      else
      {
        CAPTURE_PARSE_POS(e);
      }
    }
    else
      config_setting_set_bool(ctx->setting, (int)$1);
  }
  | TOK_INTEGER
  {
    if(IN_ARRAY() || IN_LIST())
    {
      config_setting_t *e = config_setting_set_int_elem(ctx->parent, -1, $1);
      if(! e)
      {
        libconfig_yyerror(scanner, ctx, err_array_elem_type);
        YYABORT;
      }
      else
      {
        config_setting_set_format(e, CONFIG_FORMAT_DEFAULT);
        CAPTURE_PARSE_POS(e);
      }
    }
    else
    {
      config_setting_set_int(ctx->setting, $1);
      config_setting_set_format(ctx->setting, CONFIG_FORMAT_DEFAULT);
    }
  }
  | TOK_HEX
  {
    if(IN_ARRAY() || IN_LIST())
    {
      config_setting_t *e = config_setting_set_int_elem(ctx->parent, -1, $1);
      if(! e)
      {
        libconfig_yyerror(scanner, ctx, err_array_elem_type);
        YYABORT;
      }
      else
      {
        config_setting_set_format(e, CONFIG_FORMAT_HEX);
        CAPTURE_PARSE_POS(e);
      }
    }
    else
    {
      config_setting_set_int(ctx->setting, $1);
      config_setting_set_format(ctx->setting, CONFIG_FORMAT_HEX);
    }
  }
  | TOK_FLOAT
  {
    if(IN_ARRAY() || IN_LIST())
    {
      config_setting_t *e = config_setting_set_float_elem(ctx->parent, -1, $1);
      if(! e)
      {
        libconfig_yyerror(scanner, ctx, err_array_elem_type);
        YYABORT;
      }
      else
      {
        CAPTURE_PARSE_POS(e);
      }
    }
    else
      config_setting_set_float(ctx->setting, $1);
  }
  | TOK_STRING
  {
    if(IN_ARRAY() || IN_LIST())
    {
      config_setting_t *e = config_setting_set_string_elem(ctx->parent, -1,
                                                           $1);
      free($1);
      
      if(! e)
      {
        libconfig_yyerror(scanner, ctx, err_array_elem_type);
        YYABORT;
      }
      else
      {
        CAPTURE_PARSE_POS(e);
      }
    }
    else
    {
      config_setting_set_string(ctx->setting, $1);
      free($1);
    }
  }
  ;

value_list:
    value
  | value_list TOK_COMMA value
  ;

value_list_optional:
    /* empty */
  | value_list
  ;

simple_value_list:
    simple_value
  | simple_value_list TOK_COMMA simple_value
  ;

simple_value_list_optional:
    /* empty */
  | simple_value_list
  ;

group:
  TOK_GROUP_START
  {
    if(IN_LIST())
    {
      ctx->parent = config_setting_add(ctx->parent, NULL, CONFIG_TYPE_GROUP);
      CAPTURE_PARSE_POS(ctx->parent);
    }
    else
    {
      ctx->setting->type = CONFIG_TYPE_GROUP;
      ctx->parent = ctx->setting;
      ctx->setting = NULL;
    }
  }
  setting_list_optional
  TOK_GROUP_END
  {
    if(ctx->parent)
      ctx->parent = ctx->parent->parent;
  }
  ;

%%
